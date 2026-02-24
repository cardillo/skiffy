// workqueue.cpp
//
// Distributed work queue built on raftpp.
//
// Run one process per node:
//
//   terminal 1:  ./workqueue 1 9001
//   terminal 2:  ./workqueue 2 9002 127.0.0.1:9001
//   terminal 3:  ./workqueue 3 9003 127.0.0.1:9001
//
// Each node periodically enqueues a job.  The leader
// immediately marks each committed job complete.  All
// nodes print the queue state on every change.
// Ctrl-C to stop.

#include "raftpp.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <deque>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

// -------------------------------------------------------
// command types
// -------------------------------------------------------

enum class wq_op : uint8_t {
    enqueue  = 0,
    complete = 1,
};

struct wq_cmd {
    wq_op       op      = wq_op::enqueue;
    uint64_t    job_id  = 0;
    std::string payload;

    template<typename Packer>
    void msgpack_pack(Packer& pk) const {
        pk.pack_array(3);
        pk.pack(static_cast<uint8_t>(op));
        pk.pack(job_id);
        pk.pack(payload);
    }

    void msgpack_unpack(
        msgpack::object const& o)
    {
        auto& a = o.via.array;
        op      = static_cast<wq_op>(
                      a.ptr[0].as<uint8_t>());
        job_id  = a.ptr[1].as<uint64_t>();
        payload = a.ptr[2].as<std::string>();
    }
};

// -------------------------------------------------------
// encode / decode
// -------------------------------------------------------

static std::string encode(const wq_cmd& c) {
    msgpack::sbuffer buf;
    msgpack::pack(buf, c);
    return {buf.data(), buf.data() + buf.size()};
}

static wq_cmd decode(const std::string& s) {
    auto oh = msgpack::unpack(
        s.data(), s.size());
    wq_cmd c;
    oh.get().convert(c);
    return c;
}

// -------------------------------------------------------
// output
// -------------------------------------------------------

static void print_queue(
    raftpp::server_id id,
    const std::deque<wq_cmd>& pending,
    const std::vector<wq_cmd>& done)
{
    static const char* const colors[] = {
        "\033[31m", "\033[32m", "\033[33m",
        "\033[34m", "\033[35m", "\033[36m",
    };
    const char* col = colors[(id - 1) % 6];

    std::cout << col << "node " << id;

    std::cout << "  pending[" << pending.size()
              << "]:";
    for (auto& j : pending)
        std::cout << " " << j.job_id;

    std::cout << "  done[" << done.size() << "]:";
    for (auto& j : done)
        std::cout << " " << j.job_id;

    std::cout << "\033[0m\n";
}

// -------------------------------------------------------
// signal handling
// -------------------------------------------------------

static raftpp::cluster_node* g_node    = nullptr;
static std::atomic<bool>     g_running{true};

static void sig_handler(int) {
    g_running = false;
    if (g_node) g_node->leave();
}

// -------------------------------------------------------
// main
// -------------------------------------------------------

int main(int argc, char* argv[]) {
    using namespace raftpp;

    if (argc < 3) {
        std::cerr
            << "usage: workqueue <id> <port>"
            << " [bootstrap-addr]\n\n"
            << "example (3-node cluster):\n"
            << "  ./workqueue 1 9001\n"
            << "  ./workqueue 2 9002"
            << " 127.0.0.1:9001\n"
            << "  ./workqueue 3 9003"
            << " 127.0.0.1:9001\n";
        return 1;
    }

    server_id id   = std::stoul(argv[1]);
    uint16_t  port = static_cast<uint16_t>(
                         std::stoul(argv[2]));
    bool is_boot = (argc < 4);

    logger()->set_level(spdlog::level::info);

    cluster_node node(id, port);
    g_node = &node;
    std::signal(SIGINT, sig_handler);

    // queue state — only touched from the io
    // thread via the on_apply callback
    std::deque<wq_cmd>  pending;
    std::vector<wq_cmd> done;

    node.on_apply(
        [id, &pending, &done, &node](
            const log_entry& e)
        {
            auto cmd = decode(e.value);

            if (cmd.op == wq_op::enqueue) {
                pending.push_back(cmd);
                print_queue(id, pending, done);

                // leader drives completion
                if (node.is_leader()) {
                    wq_cmd c;
                    c.op     = wq_op::complete;
                    c.job_id = cmd.job_id;
                    node.submit(encode(c));
                }

            } else { // complete
                auto it = std::find_if(
                    pending.begin(),
                    pending.end(),
                    [&](const wq_cmd& j) {
                        return j.job_id
                               == cmd.job_id;
                    });
                if (it != pending.end())
                    pending.erase(it);

                done.push_back(cmd);
                if (done.size() > 5)
                    done.erase(done.begin());

                print_queue(id, pending, done);
            }
        });

    if (!is_boot)
        node.join(argv[3]);

    // submit thread: each node periodically
    // enqueues a new job
    uint64_t next_id =
        id * 100000; // avoid id collisions
    std::thread submit_th(
        [&node, id, &next_id] {
            std::mt19937 rng(
                std::random_device{}()
                ^ static_cast<uint32_t>(id));
            std::uniform_int_distribution<int>
                ms_dist(1000, 4000);

            while (g_running) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(
                        ms_dist(rng)));
                if (!g_running) break;

                wq_cmd c;
                c.op      = wq_op::enqueue;
                c.job_id  = next_id++;
                c.payload =
                    "job from node "
                    + std::to_string(id);
                node.submit(encode(c));
            }
        });

    node.run();
    submit_th.join();
    return 0;
}
