// threads.cpp
//
// demonstrates a 3-node skiffy cluster running on separate
// threads, connected via an in-process thread-safe transport.
// no networking required.

#include <atomic>
#include <chrono>
#include <thread>

#include "spdlog/sinks/stdout_color_sinks.h"

#include "skiffy.hpp"

using namespace std::chrono_literals;

int main() {
    auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    sink->set_level(spdlog::level::warn);
    spdlog::set_default_logger(std::make_shared<spdlog::logger>("", sink));

    auto id1 = skiffy::make_node_id();
    auto id2 = skiffy::make_node_id();
    auto id3 = skiffy::make_node_id();

    skiffy::node<std::string, skiffy::channel_transport> nd1{id1};
    skiffy::node<std::string, skiffy::channel_transport> nd2{id2};
    skiffy::node<std::string, skiffy::channel_transport> nd3{id3};

    std::atomic<int> applied{0};
    auto on_apply = [&applied](const std::string& cmd) {
        fmt::print("applied: {}\n", cmd);
        ++applied;
    };
    nd1.on_apply(on_apply);
    nd2.on_apply(on_apply);
    nd3.on_apply(on_apply);

    // n2 and n3 join n1's cluster
    nd2.join(id1);
    nd3.join(id1);

    std::thread th1([&nd1] { nd1.run(); });
    std::thread th2([&nd2] { nd2.run(); });
    std::thread th3([&nd3] { nd3.run(); });

    // wait for election and membership to stabilise
    std::this_thread::sleep_for(1s);

    nd1.submit("hello");
    nd1.submit("world");

    // wait for commits to propagate
    std::this_thread::sleep_for(500ms);

    nd1.stop();
    nd2.stop();
    nd3.stop();
    th1.join();
    th2.join();
    th3.join();

    fmt::print("total applied across cluster: {}\n", applied.load());
    return 0;
}
