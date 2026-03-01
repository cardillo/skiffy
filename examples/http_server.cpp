// http_server.cpp
// HTTP KV store built on raftpp

#include <atomic>
#include <chrono>
#include <cxxopts.hpp>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

#include "httplib.h"
#include "raftpp.h"

enum class kv_op : uint8_t { set = 0, del = 1 };

struct kv_cmd {
    uint64_t req_id;
    uint8_t op;
    std::string key;
    std::string value;

    MSGPACK_DEFINE(req_id, op, key, value)
};

int main(int argc, char* argv[]) {
    try {
        cxxopts::Options opts("http_server", "HTTP KV store");
        opts.add_options()(
            "port", "raft tcp port",
            cxxopts::value<uint16_t>())(
            "host", "bind address",
            cxxopts::value<std::string>()->default_value("0.0.0.0"))(
            "http-port", "HTTP server port (raft_port + 1000)",
            cxxopts::value<uint16_t>()->default_value("0"))(
            "bootstrap", "host:port of existing node",
            cxxopts::value<std::string>()->default_value(""))(
            "persistent", "use file-based log store",
            cxxopts::value<bool>()->default_value("false"))(
            "compact", "compact threshold (entries)",
            cxxopts::value<size_t>()->default_value("1000"))(
            "h,help", "show help");

        auto result = opts.parse(argc, argv);
        if (result.count("help")) {
            std::cout << opts.help() << "\n";
            return 0;
        }
        if (!result.count("port")) {
            std::cerr << opts.help() << "\n";
            return 1;
        }

        uint16_t port = result["port"].as<uint16_t>();
        std::string host = result["host"].as<std::string>();
        uint16_t http_port = result["http-port"].as<uint16_t>();
        if (http_port == 0)
            http_port = port + 1000;
        std::string bootstrap = result["bootstrap"].as<std::string>();
        bool persistent = result["persistent"].as<bool>();
        size_t compact_threshold = result["compact"].as<size_t>();

        auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        auto log = std::make_shared<spdlog::logger>("http_server", sink);
        log->set_level(spdlog::level::info);
        log->set_pattern("[%T] [%^%l%$] %v");
        spdlog::register_logger(log);

        log->info("starting on {}:{} (http: {})", host, port, http_port);

        // Create cluster node
        std::unique_ptr<raftpp::cluster_node<kv_cmd>> node;
        if (persistent) {
            node = std::make_unique<raftpp::cluster_node<
                kv_cmd, raftpp::file_log_store>>(host, port);
        } else {
            node = std::make_unique<raftpp::cluster_node<kv_cmd>>(host,
                                                                   port);
        }

        if (compact_threshold > 0) {
            node->compact_threshold(compact_threshold);
        }

        // KV store
        std::unordered_map<std::string, std::string> kv;
        std::shared_mutex kv_mu;

        // Pending requests
        std::unordered_map<uint64_t, std::promise<bool>*> pending;
        std::mutex pending_mu;

        uint64_t next_req_id = 1;

        node->on_apply([&](const kv_cmd& cmd) {
            {
                std::unique_lock lk(kv_mu);
                if (cmd.op == 0)  // set
                    kv[cmd.key] = cmd.value;
                else if (cmd.op == 1)  // del
                    kv.erase(cmd.key);
            }

            {
                std::lock_guard lk(pending_mu);
                auto it = pending.find(cmd.req_id);
                if (it != pending.end()) {
                    it->second->set_value(true);
                    pending.erase(it);
                }
            }
        });

        if (!bootstrap.empty()) {
            log->info("joining cluster via {}", bootstrap);
            node->join(bootstrap);
        }

        // HTTP server
        httplib::Server svr;

        svr.Put("/kv/(.*)", [&](const httplib::Request& req,
                                 httplib::Response& res) {
            if (!node->is_leader()) {
                auto leader_addr = fmt::format("{}", node->leader_id());
                if (leader_addr.empty()) {
                    res.status = 503;
                    res.set_content("no leader", "text/plain");
                    return;
                }

                // Parse leader address
                auto sep = leader_addr.rfind(':');
                auto h = leader_addr.substr(0, sep);
                int lhttp = std::stoi(leader_addr.substr(sep + 1)) +
                            1000;

                httplib::Client fwd(h, lhttp);
                auto fwd_res = fwd.Put("/kv/" + req.matches[1].str(),
                                       req.body, "text/plain");
                if (fwd_res) {
                    res.status = fwd_res->status;
                    res.set_content(fwd_res->body, fwd_res->get_header_value(
                                                        "content-type"));
                } else {
                    res.status = 503;
                    res.set_content("forwarding failed", "text/plain");
                }
                return;
            }

            kv_cmd cmd;
            cmd.req_id = next_req_id++;
            cmd.op = 0;  // set
            cmd.key = req.matches[1].str();
            cmd.value = req.body;

            std::promise<bool> promise;
            {
                std::lock_guard lk(pending_mu);
                pending[cmd.req_id] = &promise;
            }

            node->submit(cmd);

            auto future = promise.get_future();
            auto status = future.wait_for(std::chrono::seconds(5));
            if (status == std::future_status::timeout) {
                {
                    std::lock_guard lk(pending_mu);
                    pending.erase(cmd.req_id);
                }
                res.status = 503;
                res.set_content("timeout", "text/plain");
            } else {
                res.status = 200;
                res.set_content("ok", "text/plain");
            }
        });

        svr.Delete("/kv/(.*)", [&](const httplib::Request& req,
                                    httplib::Response& res) {
            if (!node->is_leader()) {
                auto leader_addr = fmt::format("{}", node->leader_id());
                if (leader_addr.empty()) {
                    res.status = 503;
                    res.set_content("no leader", "text/plain");
                    return;
                }

                auto sep = leader_addr.rfind(':');
                auto h = leader_addr.substr(0, sep);
                int lhttp = std::stoi(leader_addr.substr(sep + 1)) +
                            1000;

                httplib::Client fwd(h, lhttp);
                auto fwd_res =
                    fwd.Delete("/kv/" + req.matches[1].str());
                if (fwd_res) {
                    res.status = fwd_res->status;
                    res.set_content(fwd_res->body, fwd_res->get_header_value(
                                                        "content-type"));
                } else {
                    res.status = 503;
                    res.set_content("forwarding failed", "text/plain");
                }
                return;
            }

            kv_cmd cmd;
            cmd.req_id = next_req_id++;
            cmd.op = 1;  // del
            cmd.key = req.matches[1].str();
            cmd.value = "";

            std::promise<bool> promise;
            {
                std::lock_guard lk(pending_mu);
                pending[cmd.req_id] = &promise;
            }

            node->submit(cmd);

            auto future = promise.get_future();
            auto status = future.wait_for(std::chrono::seconds(5));
            if (status == std::future_status::timeout) {
                {
                    std::lock_guard lk(pending_mu);
                    pending.erase(cmd.req_id);
                }
                res.status = 503;
                res.set_content("timeout", "text/plain");
            } else {
                res.status = 200;
                res.set_content("ok", "text/plain");
            }
        });

        svr.Get("/kv/(.*)", [&](const httplib::Request& req,
                                 httplib::Response& res) {
            std::shared_lock lk(kv_mu);
            auto it = kv.find(req.matches[1].str());
            if (it != kv.end()) {
                res.status = 200;
                res.set_content(it->second, "text/plain");
            } else {
                res.status = 404;
                res.set_content("not found", "text/plain");
            }
        });

        svr.Get("/status", [&](const httplib::Request&,
                                httplib::Response& res) {
            std::string state_str;
            switch (node->state()) {
                case raftpp::server_state::follower:
                    state_str = "follower";
                    break;
                case raftpp::server_state::candidate:
                    state_str = "candidate";
                    break;
                case raftpp::server_state::leader:
                    state_str = "leader";
                    break;
            }
            res.status = 200;
            res.set_content(
                "{\"addr\":\"" + fmt::format("{}", node->leader_id()) +
                    "\",\"state\":\"" + state_str + "\"}",
                "application/json");
        });

        // Run Raft in background thread
        std::thread raft_th([&] { node->run(); });

        // Run HTTP server
        log->info("HTTP server listening on 0.0.0.0:{}", http_port);
        svr.listen("0.0.0.0", http_port);

        node->leave();
        raft_th.join();
        log->info("shutdown complete");

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
}
