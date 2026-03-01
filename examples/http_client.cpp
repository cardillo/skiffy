// http_client.cpp
// HTTP load test client for KV server

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cxxopts.hpp>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "httplib.h"

std::string rand_str(int n, std::mt19937& rng) {
    static const char cs[] = "abcdefghijklmnopqrstuvwxyz0123456789";
    std::uniform_int_distribution<> d(0, sizeof(cs) - 2);
    std::string s(n, ' ');
    for (auto& c : s)
        c = cs[d(rng)];
    return s;
}

int main(int argc, char* argv[]) {
    try {
        cxxopts::Options opts("http_client", "HTTP KV load test");
        opts.add_options()("servers", "comma-separated list of host:port",
                           cxxopts::value<std::string>())(
            "connections", "number of concurrent clients",
            cxxopts::value<int>()->default_value("10"))(
            "duration", "seconds to run",
            cxxopts::value<int>()->default_value("10"))(
            "payload-size", "bytes per value",
            cxxopts::value<int>()->default_value("256"))(
            "warmup", "seconds to warm up before measuring",
            cxxopts::value<int>()->default_value("2"))("h,help", "show help");

        auto result = opts.parse(argc, argv);
        if (result.count("help")) {
            std::cout << opts.help() << "\n";
            return 0;
        }
        if (!result.count("servers")) {
            std::cerr << opts.help() << "\n";
            return 1;
        }

        std::vector<std::pair<std::string, int>> servers;
        std::string servers_str = result["servers"].as<std::string>();
        size_t pos = 0;
        while (pos < servers_str.size()) {
            size_t sep = servers_str.find(',', pos);
            if (sep == std::string::npos)
                sep = servers_str.size();
            std::string addr = servers_str.substr(pos, sep - pos);
            size_t colon = addr.rfind(':');
            std::string host = addr.substr(0, colon);
            int port = std::stoi(addr.substr(colon + 1));
            servers.push_back({host, port});
            pos = sep + 1;
        }

        int n_connections = result["connections"].as<int>();
        int duration = result["duration"].as<int>();
        int payload_size = result["payload-size"].as<int>();
        int warmup = result["warmup"].as<int>();

        std::cout << "duration: " << duration
                  << "s  connections: " << n_connections
                  << "  payload: " << payload_size << " B\n";

        std::atomic<uint64_t> total_ops{0};
        std::atomic<uint64_t> total_errors{0};
        std::mutex lat_mu;
        std::vector<int64_t> all_latencies;

        auto client_fn = [&](int client_id) {
            std::mt19937 rng(std::random_device{}() ^ client_id);
            std::uniform_int_distribution<> key_dist(0, 999);

            // Pre-generate 1000 keys
            std::vector<std::string> keys;
            for (int i = 0; i < 1000; ++i)
                keys.push_back(rand_str(8, rng));

            std::vector<int64_t> latencies;
            uint64_t ops = 0;
            uint64_t errors = 0;

            int server_idx = client_id % servers.size();
            auto [host, port] = servers[server_idx];
            httplib::Client cli(host, port);
            cli.set_connection_timeout(0, 500000);
            cli.set_read_timeout(5, 0);
            cli.set_write_timeout(5, 0);

            auto start = std::chrono::steady_clock::now();
            auto warmup_end = start + std::chrono::seconds(warmup);
            auto end = start + std::chrono::seconds(duration + warmup);

            while (std::chrono::steady_clock::now() < end) {
                auto now = std::chrono::steady_clock::now();
                bool measuring = now > warmup_end;

                std::string key = keys[key_dist(rng)];
                std::string value = rand_str(payload_size, rng);

                auto t0 = std::chrono::steady_clock::now();
                auto res = cli.Put("/kv/" + key, value, "text/plain");
                auto t1 = std::chrono::steady_clock::now();

                int64_t lat_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(t1 -
                                                                          t0)
                        .count();

                if (measuring) {
                    ++ops;
                    if (!res || res->status != 200)
                        ++errors;
                    else
                        latencies.push_back(lat_us);
                }

                if (res && res->status == 503) {
                    // No leader; rotate to next server and wait
                    server_idx = (server_idx + 1) % servers.size();
                    auto [h, p] = servers[server_idx];
                    cli = httplib::Client(h, p);
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(50));
                }
            }

            total_ops += ops;
            total_errors += errors;
            {
                std::lock_guard lk(lat_mu);
                all_latencies.insert(all_latencies.end(), latencies.begin(),
                                     latencies.end());
            }
        };

        std::vector<std::thread> threads;
        for (int i = 0; i < n_connections; ++i)
            threads.emplace_back(client_fn, i);

        for (auto& th : threads)
            th.join();

        // Compute percentiles
        std::sort(all_latencies.begin(), all_latencies.end());

        int64_t p50 = 0, p95 = 0, p99 = 0, max_lat = 0;
        if (!all_latencies.empty()) {
            p50 = all_latencies[all_latencies.size() / 2];
            p95 = all_latencies[all_latencies.size() * 95 / 100];
            p99 = all_latencies[all_latencies.size() * 99 / 100];
            max_lat = all_latencies.back();
        }

        double throughput = total_ops / static_cast<double>(duration);

        std::cout << std::fixed << std::setprecision(0);
        std::cout << "ops: " << total_ops << "  errors: " << total_errors
                  << "  throughput: " << throughput << " ops/sec\n";
        std::cout << "latency (us):  p50=" << p50 << "  p95=" << p95
                  << "  p99=" << p99 << "  max=" << max_lat << "\n";

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
}
