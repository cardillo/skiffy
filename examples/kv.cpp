// kv.cpp
//
// Run one process per node.
//
// Bootstrap (first node, 3-node cluster):
//   ./kv 1 9001 192.168.1.10 3
//
// Join:
//   ./kv 2 9002 192.168.1.11 3 192.168.1.10:9001
//   ./kv 3 9003 192.168.1.12 3 192.168.1.10:9001
//
// Arguments:
//   id             unique server id (integer >= 1)
//   port           raft TCP port
//   host           advertise address (reachable
//                  by all other nodes)
//   expected       expected cluster size
//                  (default: 1)
//   bootstrap-addr host:port of an existing node
//
// Each node periodically submits a random set
// command.  The store is capped at 5 entries.
// Ctrl-C to stop.

#include "raftpp.h"

#include <chrono>
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
        const std::unordered_map<int,int>& kv) {
    static const char* const colors[] = {
        "\033[31m", "\033[32m", "\033[33m",
        "\033[34m", "\033[35m", "\033[36m",
    };
    const char* col = colors[(id - 1) % 6];
    std::cout
        << col << "node " << id
        << " store: {";
    bool first = true;
    for (auto& [k, v] : kv) {
        if (!first) std::cout << ", ";
        std::cout << k << ":" << v;
        first = false;
    }
    std::cout << "}\033[0m\n";
}

// -------------------------------------------------------
// main
// -------------------------------------------------------

int main(int argc, char* argv[]) {
    using namespace raftpp;

    if (argc < 4) {
        std::cerr
            << "usage: kv <id> <port> <host>"
            << " [expected] [bootstrap-addr]\n\n"
            << "  host           advertise"
            << " address (reachable by all"
            << " nodes)\n"
            << "  expected       expected"
            << " cluster size (default: 1)\n"
            << "  bootstrap-addr host:port"
            << " of an existing node\n\n"
            << "examples (3-node cluster):\n"
            << "  ./kv 1 9001"
            << " 192.168.1.10 3\n"
            << "  ./kv 2 9002"
            << " 192.168.1.11 3"
            << " 192.168.1.10:9001\n"
            << "  ./kv 3 9003"
            << " 192.168.1.12 3"
            << " 192.168.1.10:9001\n";
        return 1;
    }

    server_id   id   = std::stoul(argv[1]);
    uint16_t    port = static_cast<uint16_t>(
                           std::stoul(argv[2]));
    std::string host = argv[3];

    // argv[4+] is either expected-size (no ':')
    // or bootstrap-addr (contains ':')
    size_t expected   = 1;
    std::string bootstrap;

    for (int i = 4; i < argc; ++i) {
        std::string arg(argv[i]);
        if (arg.find(':') != std::string::npos)
            bootstrap = arg;
        else
            expected = std::stoul(arg);
    }

    // minimum peers needed before submitting
    size_t quorum = expected / 2 + 1;

    logger()->set_level(spdlog::level::info);

    cluster_node node(id, port, host);

    // kv store — only touched from the io
    // thread via the on_apply callback
    std::unordered_map<int,int> kv;

    node.on_apply(
        [id, &kv, &node](
            const log_entry& e)
        {
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

    if (!bootstrap.empty())
        node.join(bootstrap);

    // submit thread: wait for quorum then
    // periodically submit a random set command
    std::thread submit_th(
        [&node, id, quorum] {
            std::mt19937 rng(
                std::random_device{}()
                ^ static_cast<uint32_t>(id));
            std::uniform_int_distribution<int>
                key_dist(1, 20);
            std::uniform_int_distribution<int>
                val_dist(1, 999);
            std::uniform_int_distribution<int>
                ms_dist(500, 3000);

            while (node.running()) {
                // wait until cluster has quorum
                if (node.peer_count() + 1
                        < quorum) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(
                            200));
                    continue;
                }

                std::this_thread::sleep_for(
                    std::chrono::milliseconds(
                        ms_dist(rng)));
                if (!node.running()) break;

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
