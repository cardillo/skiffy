// http_client.cpp
// HTTP load test client for KV server

#define ANKERL_NANOBENCH_IMPLEMENT
#include <algorithm>
#include <cxxopts.hpp>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "httplib.h"
#include "nanobench.h"

namespace nb = ankerl::nanobench;
using M = nb::Result::Measure;

static std::string rand_str(int n, std::mt19937& rng) {
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
            "connections", "connection pool size",
            cxxopts::value<int>()->default_value("10"))(
            "requests", "requests per benchmark",
            cxxopts::value<int>()->default_value("1000"))(
            "payload-size", "bytes per value",
            cxxopts::value<int>()->default_value("256"))(
            "warmup", "warmup requests before measuring",
            cxxopts::value<int>()->default_value("100"))("h,help",
                                                         "show help");

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
        {
            auto s = result["servers"].as<std::string>();
            size_t pos = 0;
            while (pos < s.size()) {
                auto sep = s.find(',', pos);
                if (sep == std::string::npos)
                    sep = s.size();
                auto addr = s.substr(pos, sep - pos);
                auto col = addr.rfind(':');
                servers.push_back(
                    {addr.substr(0, col), std::stoi(addr.substr(col + 1))});
                pos = sep + 1;
            }
        }

        auto n_conn = result["connections"].as<int>();
        auto n_req = result["requests"].as<int>();
        auto psize = result["payload-size"].as<int>();
        auto n_warm = result["warmup"].as<int>();

        std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<> kd(0, 999);

        std::vector<std::string> keys;
        for (int i = 0; i < 1000; ++i)
            keys.push_back(rand_str(8, rng));

        using Cli = std::unique_ptr<httplib::Client>;
        std::vector<Cli> pool;
        for (int i = 0; i < n_conn; ++i) {
            auto [host, port] = servers[i % (int)servers.size()];
            auto c = std::make_unique<httplib::Client>(host, port);
            c->set_connection_timeout(0, 500000);
            c->set_read_timeout(5, 0);
            c->set_write_timeout(5, 0);
            pool.push_back(std::move(c));
        }

        std::cout << "http kv load test"
                  << "  connections: " << n_conn << "  requests: " << n_req
                  << "  payload: " << psize << "b\n";

        nb::Bench bench;
        bench.performanceCounters(false)
            .timeUnit(std::chrono::milliseconds(1), "ms")
            .epochs(static_cast<size_t>(10))
            .minEpochIterations(n_req / 10)
            .warmup(static_cast<uint64_t>(n_warm));

        int ci = 0;
        std::map<std::string, int> put_errs;
        bench.run("PUT /kv", [&] {
            auto& cli = *pool[ci++ % n_conn];
            std::string key = keys[kd(rng)];
            std::string val = rand_str(psize, rng);
            auto res = cli.Put("/kv/" + key, val, "text/plain");
            if (!res)
                put_errs["no response"]++;
            else if (res->status != 200)
                put_errs["http " + std::to_string(res->status)]++;
        });

        ci = 0;
        std::map<std::string, int> get_errs;
        bench.run("GET /kv", [&] {
            auto& cli = *pool[ci++ % n_conn];
            std::string key = keys[kd(rng)];
            auto res = cli.Get("/kv/" + key);
            if (!res)
                get_errs["no response"]++;
            else if (res->status != 200 && res->status != 404)
                get_errs["http " + std::to_string(res->status)]++;
        });

        /*
        auto& bres = bench.results();
        std::cout << "\nwrite (PUT):\n";
        print_bench(bres[0], put_errs);
        std::cout << "\nread (GET):\n";
        print_bench(bres[1], get_errs);
         */

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
}
