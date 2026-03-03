// queue.cpp
//
// distributed work queue built on raftpp
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

int main(int argc, char* argv[]) {
    try {
        cxxopts::Options opts("queue", "distributed work queue");
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

        raftpp::cluster_node<wq_cmd, raftpp::memory_log_store> node(id);

        // queue state — only touched from the io
        // thread via the on_apply callback
        std::deque<wq_cmd> pending;
        std::vector<wq_cmd> done;

        node.on_drop([log](const wq_cmd& cmd) {
            log->warn("dropped: {} job_id={}",
                      cmd.op == wq_op::enqueue ? "enqueue" : "complete",
                      cmd.job_id);
        });

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

        // submit thread: each node periodically enqueues a new job;
        // also drives the optional timeout
        uint64_t next_id = port * 100000; // avoid id collisions
        std::thread submit_th([&node, &id, secs, log, &next_id] {
            std::mt19937 rng(std::random_device{}() ^
                             static_cast<uint32_t>(id.port_));
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

                node.submit({wq_op::enqueue, next_id++,
                             "job from node " + std::string(id)});
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
