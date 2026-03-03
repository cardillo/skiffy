// kv.cpp
//
// distributed key-value store built using raftpp
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

int main(int argc, char* argv[]) {
    try {
        cxxopts::Options opts("kv", "distributed key-value store");
        opts.add_options()("port", "raft tcp port",
                           cxxopts::value<uint16_t>())(
            "host", "advertise address",
            cxxopts::value<std::string>()->default_value(
                asio::ip::host_name()))(
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
        if (!result.count("port")) {
            std::cerr << opts.help() << "\n";
            return 1;
        }

        auto port = result["port"].as<uint16_t>();
        auto host = result["host"].as<std::string>();
        auto bootstrap = result["bootstrap"].as<std::string>();
        auto secs = result["timeout"].as<uint32_t>();

        auto id = raftpp::resolve_server_id(host, port);

        // per-node color; library + app logs share this logger
        const auto col = "\033[3" + std::to_string(port % 6 + 1) + "m";
        std::string prefix = col + "[" + std::string(id) + "]\033[0m";

        auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        auto log = std::make_shared<spdlog::logger>("raftpp", sink);
        log->set_level(spdlog::level::info);
        log->set_pattern(prefix + " [%T] [%^%l%$] %v");
        spdlog::register_logger(log);

        log->info("starting");

        raftpp::cluster_node<kv_cmd, raftpp::memory_log_store> node(id);

        // kv store — only touched from the io
        // thread via the on_apply callback
        std::unordered_map<int, int> kv;

        node.on_drop([log](const kv_cmd& cmd) {
            log->warn("dropped: {} key={} val={}",
                      cmd.op == kv_op::set ? "set" : "del", cmd.key, cmd.val);
        });

        node.on_apply([&kv, &node, log](const kv_cmd& cmd) {
            switch (cmd.op) {
                case kv_op::set:
                    kv[cmd.key] = cmd.val;
                    // leader trims store to 5
                    if (kv.size() > 5 && node.is_leader()) {
                        node.submit({kv_op::del, kv.begin()->first});
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

        // submit thread: periodically submit a random set command;
        // also drives the optional timeout
        std::thread submit_th([&node, &id, secs, log] {
            std::mt19937 rng(std::random_device{}() ^
                             static_cast<uint32_t>(id.port_));
            std::uniform_int_distribution<int> key_dist(1, 20);
            std::uniform_int_distribution<int> val_dist(1, 999);
            std::uniform_int_distribution<int> ms_dist(3000, 9000);
            auto start = std::chrono::steady_clock::now();

            while (node.running()) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(ms_dist(rng)));
                if (!node.running())
                    break;

                if (secs > 0) {
                    auto elapsed = std::chrono::steady_clock::now() - start;
                    if (elapsed >= std::chrono::seconds(secs)) {
                        log->info("timeout reached, stopping");
                        node.leave();
                        break;
                    }
                }

                node.submit({kv_op::set, key_dist(rng), val_dist(rng)});
            }
        });

        node.run();
        log->info("shutdown complete");
        submit_th.join();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
}
