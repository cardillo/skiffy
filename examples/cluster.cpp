// cluster.cpp
//
// Run one process per node:
//
//   terminal 1:  ./cluster 1 9001
//   terminal 2:  ./cluster 2 9002 127.0.0.1:9001
//   terminal 3:  ./cluster 3 9003 127.0.0.1:9001
//
// Each node periodically submits a random set command.
// The store is capped at 5 entries: the leader submits
// a del command when the cap is exceeded.  Every node
// prints the full store on each change.  Ctrl-C to stop.

#include "raftpp.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>

// -------------------------------------------------------
// kv command types
// -------------------------------------------------------

enum class kv_op : uint8_t { set = 0, del = 1 };

struct kv_cmd {
    kv_op op;
    int   key;
    int   val = 0;  // set only

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        pk.pack_array(3);
        pk.pack(static_cast<uint8_t>(op));
        pk.pack(key);
        pk.pack(val);
    }

    void msgpack_unpack(msgpack::object const& o) {
        auto& a = o.via.array;
        op  = static_cast<kv_op>(
                  a.ptr[0].as<uint8_t>());
        key = a.ptr[1].as<int>();
        val = a.ptr[2].as<int>();
    }
};

// -------------------------------------------------------
// encode / decode
// -------------------------------------------------------

static std::string encode_cmd(const kv_cmd& c) {
    msgpack::sbuffer buf;
    msgpack::pack(buf, c);
    return {buf.data(), buf.data() + buf.size()};
}

static kv_cmd decode_cmd(const std::string& s) {
    auto oh = msgpack::unpack(s.data(), s.size());
    kv_cmd c;
    oh.get().convert(c);
    return c;
}

// -------------------------------------------------------
// output
// -------------------------------------------------------

static void print_kv(
    raftpp::server_id id,
    const std::unordered_map<int,int>& kv)
{
    static const char* const colors[] = {
        "\033[31m", "\033[32m", "\033[33m",
        "\033[34m", "\033[35m", "\033[36m",
    };
    const char* col = colors[(id - 1) % 6];
    std::cout << col << "node " << id << " store: {";
    bool first = true;
    for (auto& [k, v] : kv) {
        if (!first) std::cout << ", ";
        std::cout << k << ":" << v;
        first = false;
    }
    std::cout << "}\033[0m\n";
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
            << "usage: cluster <id> <port>"
            << " [bootstrap-addr]\n\n"
            << "example (3-node cluster):\n"
            << "  ./cluster 1 9001\n"
            << "  ./cluster 2 9002"
            << " 127.0.0.1:9001\n"
            << "  ./cluster 3 9003"
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

    // kv store — only touched from the io thread
    // via the on_apply callback
    std::unordered_map<int,int> kv;

    node.on_apply(
        [id, &kv, &node](const log_entry& e) {
            auto cmd = decode_cmd(e.value);
            switch (cmd.op) {
            case kv_op::set:
                kv[cmd.key] = cmd.val;
                // leader trims store to 5
                if (kv.size() > 5
                    && node.is_leader()) {
                    kv_cmd d;
                    d.op  = kv_op::del;
                    d.key = kv.begin()->first;
                    node.submit(encode_cmd(d));
                }
                break;
            case kv_op::del:
                kv.erase(cmd.key);
                break;
            }
            print_kv(id, kv);
        });

    if (!is_boot)
        node.join(argv[3]);

    // submit thread: each node periodically
    // submits a random set command
    std::thread submit_th([&node, id] {
        std::mt19937 rng(
            std::random_device{}()
            ^ static_cast<uint32_t>(id));
        std::uniform_int_distribution<int>
            key_dist(1, 20);
        std::uniform_int_distribution<int>
            val_dist(1, 999);
        std::uniform_int_distribution<int>
            ms_dist(500, 3000);

        while (g_running) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(
                    ms_dist(rng)));
            if (!g_running) break;
            kv_cmd c;
            c.op  = kv_op::set;
            c.key = key_dist(rng);
            c.val = val_dist(rng);
            node.submit(encode_cmd(c));
        }
    });

    node.run();
    submit_th.join();
    return 0;
}
