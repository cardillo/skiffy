// kv.cpp
//
// Run one process per node.
//
// Each node periodically submits a random set
// command.  The store is capped at 5 entries.
// Ctrl-C to stop.

#include <chrono>
#include <cxxopts.hpp>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>

#include "spdlog/fmt/ranges.h"
#include "spdlog/sinks/stdout_color_sinks.h"

#include "raftpp.h"

// -------------------------------------------------------
// kv command types
// -------------------------------------------------------

enum class kv_op : uint8_t { set = 0, del = 1 };

struct kv_cmd {
    kv_op op;
    int key;
    int val = 0; // used for set only

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        pk.pack_array(3);
        pk.pack(static_cast<uint8_t>(op));
        pk.pack(key);
        pk.pack(val);
    }

    void msgpack_unpack(msgpack::object const& o) {
        auto& a = o.via.array;
        op = static_cast<kv_op>(a.ptr[0].as<uint8_t>());
        key = a.ptr[1].as<int>();
        val = a.ptr[2].as<int>();
    }
};

// -------------------------------------------------------
// main
// -------------------------------------------------------

int main(int argc, char* argv[]) {
    try {
        cxxopts::Options opts("kv", "distributed key-value store");
        opts.add_options()("id", "server id", cxxopts::value<uint64_t>())(
            "port", "raft tcp port (default: 9000+id)",
            cxxopts::value<uint16_t>())(
            "host", "advertise address",
            cxxopts::value<std::string>()->default_value(
                asio::ip::host_name()))(
            "expected", "expected cluster size",
            cxxopts::value<size_t>()->default_value("3"))(
            "bootstrap", "host:port of existing node",
            cxxopts::value<std::string>()->default_value(""))(
            "timeout", "seconds to run (0 = indefinite)",
            cxxopts::value<uint32_t>()->default_value("0"))("h,help",
                                                            "show help");

        auto result = opts.parse(argc, argv);
        if (result.count("help")) {
            std::cout << opts.help() << "\n";
            return 0;
        }
        if (!result.count("id")) {
            std::cerr << opts.help() << "\n";
            return 1;
        }

        raftpp::server_id id = result["id"].as<uint64_t>();
        uint16_t port = result.count("port") ?
            result["port"].as<uint16_t>() :
            static_cast<uint16_t>(9000 + id);
        std::string host = result["host"].as<std::string>();
        size_t expected = result["expected"].as<size_t>();
        std::string bootstrap = result["bootstrap"].as<std::string>();
        uint32_t secs = result["timeout"].as<uint32_t>();

        // per-node color; library + app logs share this logger
        static const char* const colors[] = {
            "\033[31m", "\033[32m", "\033[33m",
            "\033[34m", "\033[35m", "\033[36m",
        };
        const char* col = colors[(id - 1) % 6];
        std::string prefix =
            std::string(col) + "[node " + std::to_string(id) + "]\033[0m";

        auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        auto log = std::make_shared<spdlog::logger>("raftpp", sink);
        log->set_level(spdlog::level::info);
        log->set_pattern(prefix + " [%T] [%^%l%$] %v");
        spdlog::register_logger(log);

        log->info("starting on {}:{} (expecting {} nodes)", host, port,
                  expected);

        raftpp::cluster_node<kv_cmd, raftpp::memory_log_store> node(
            id, host, port, expected);

        // kv store — only touched from the io
        // thread via the on_apply callback
        std::unordered_map<int, int> kv;

        node.on_apply([&kv, &node, log](const kv_cmd& cmd) {
            switch (cmd.op) {
                case kv_op::set:
                    kv[cmd.key] = cmd.val;
                    // leader trims store to 5
                    if (kv.size() > 5 && node.is_leader()) {
                        kv_cmd d;
                        d.op = kv_op::del;
                        d.key = kv.begin()->first;
                        node.submit(d);
                    }
                    break;
                case kv_op::del:
                    kv.erase(cmd.key);
                    break;
            }
            log->info("store: {}", kv);
        });

        if (!bootstrap.empty()) {
            log->info("joining cluster via {}", bootstrap);
            node.join(bootstrap);
        }

        // submit thread: periodically submit a random set command
        std::thread submit_th([&node, id] {
            std::mt19937 rng(std::random_device{}() ^
                             static_cast<uint32_t>(id));
            std::uniform_int_distribution<int> key_dist(1, 20);
            std::uniform_int_distribution<int> val_dist(1, 999);
            std::uniform_int_distribution<int> ms_dist(1000, 3000);

            while (node.running()) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(ms_dist(rng)));
                if (!node.running())
                    break;

                kv_cmd c;
                c.op = kv_op::set;
                c.key = key_dist(rng);
                c.val = val_dist(rng);
                node.submit(c);
            }
        });

        std::thread timer_th;
        if (secs > 0) {
            timer_th = std::thread([&node, secs, log] {
                std::this_thread::sleep_for(std::chrono::seconds(secs));
                log->info("timeout reached, stopping");
                node.leave();
            });
        }

        node.run();
        log->info("shutdown complete");
        submit_th.join();
        if (timer_th.joinable())
            timer_th.join();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
}
