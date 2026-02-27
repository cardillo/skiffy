// queue.cpp
//
// Distributed work queue built on raftpp.
//
// Each node periodically enqueues a job.  The
// leader immediately marks each committed job
// complete.  All nodes log the queue state on
// every change.  Ctrl-C to stop.

#include <algorithm>
#include <chrono>
#include <cxxopts.hpp>
#include <deque>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "spdlog/fmt/ranges.h"
#include "spdlog/sinks/stdout_color_sinks.h"

#include "raftpp.h"

// -------------------------------------------------------
// command types
// -------------------------------------------------------

enum class wq_op : uint8_t {
    enqueue = 0,
    complete = 1,
};

struct wq_cmd {
    wq_op op = wq_op::enqueue;
    uint64_t job_id = 0;
    std::string payload;

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        pk.pack_array(3);
        pk.pack(static_cast<uint8_t>(op));
        pk.pack(job_id);
        pk.pack(payload);
    }

    void msgpack_unpack(msgpack::object const& o) {
        auto& a = o.via.array;
        op = static_cast<wq_op>(a.ptr[0].as<uint8_t>());
        job_id = a.ptr[1].as<uint64_t>();
        payload = a.ptr[2].as<std::string>();
    }
};

// -------------------------------------------------------
// main
// -------------------------------------------------------

int main(int argc, char* argv[]) {
    try {
        cxxopts::Options opts("queue", "distributed work queue");
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

        raftpp::cluster_node<wq_cmd, raftpp::memory_log_store> node(
            id, host, port, expected);

        // queue state — only touched from the io
        // thread via the on_apply callback
        std::deque<wq_cmd> pending;
        std::vector<wq_cmd> done;

        node.on_apply([&pending, &done, &node, log](const wq_cmd& cmd) {
            if (cmd.op == wq_op::enqueue) {
                pending.push_back(cmd);
                log->info("enqueued job {} ({})", cmd.job_id, cmd.payload);

                // leader drives completion
                if (node.is_leader()) {
                    wq_cmd c;
                    c.op = wq_op::complete;
                    c.job_id = cmd.job_id;
                    node.submit(c);
                }
            } else {
                auto it = std::find_if(
                    pending.begin(), pending.end(),
                    [&](const wq_cmd& j) { return j.job_id == cmd.job_id; });
                if (it != pending.end())
                    pending.erase(it);

                done.push_back(cmd);
                if (done.size() > 5)
                    done.erase(done.begin());

                log->info("completed job {}", cmd.job_id);
            }

            std::vector<uint64_t> pids, dids;
            for (auto& j : pending)
                pids.push_back(j.job_id);
            for (auto& j : done)
                dids.push_back(j.job_id);
            log->info("pending:{}  done:{}", pids, dids);
        });

        if (!bootstrap.empty()) {
            log->info("joining cluster via {}", bootstrap);
            node.join(bootstrap);
        }

        // submit thread: each node periodically enqueues a new job
        uint64_t next_id = id * 100000; // avoid id collisions
        std::thread submit_th([&node, id, &next_id] {
            std::mt19937 rng(std::random_device{}() ^
                             static_cast<uint32_t>(id));
            std::uniform_int_distribution<int> ms_dist(1000, 3000);

            while (node.running()) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(ms_dist(rng)));
                if (!node.running())
                    break;

                wq_cmd c;
                c.op = wq_op::enqueue;
                c.job_id = next_id++;
                c.payload = "job from node " + std::to_string(id);
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
