#pragma once
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <utility>
#include <vector>

#include "raftpp.h"

namespace raftpp {

// -------------------------------------------------------
// memory_transport
// -------------------------------------------------------

struct memory_transport {
    std::vector<message> sent;

    void send(const message& m) { sent.push_back(m); }
    void clear() { sent.clear(); }

    template<typename Fn>
    void deliver(Fn&& fn) {
        auto msgs = sent;
        clear();
        for (auto& m : msgs)
            std::forward<Fn>(fn)(m);
    }
};

// -------------------------------------------------------
// sim_transport
// -------------------------------------------------------

struct sim_transport {
    double drop_rate = 0.0;
    double dup_rate = 0.0;

    void send(const message& m) { queue_.push_back(m); }

    void clear() { queue_.clear(); }

    const std::deque<message>& pending() const { return queue_; }
    size_t pending_count() const { return queue_.size(); }

    template <typename Fn>
    void deliver(Fn&& fn) {
        std::deque<message> tmp;
        tmp.swap(queue_);
        std::uniform_real_distribution<double> dist{0.0, 1.0};
        for (auto& msg : tmp) {
            if (dist(rng_) < drop_rate)
                continue;
            fn(msg);
            if (dist(rng_) < dup_rate)
                fn(msg);
        }
    }

  private:
    std::deque<message> queue_;
    std::mt19937 rng_{std::random_device{}()};
};

// -------------------------------------------------------
// cluster_sim
// -------------------------------------------------------
// Orchestrates N servers sharing one sim_transport.
// Provides per-node crash/restart and network partition
// to support fault-injection testing.

struct cluster_sim {
    sim_transport transport;

    // node id -> server (heap-allocated to keep stable
    // address; transport_ ref stays valid as long as
    // cluster_sim is not moved after construction)
    std::map<server_id,
        std::unique_ptr<server<sim_transport>>> nodes;

    // nodes that have been crashed (drop inbound messages)
    std::set<server_id> crashed;

    // optional partition: messages between the two sets
    // are silently dropped in both directions
    std::optional<std::pair<
        std::set<server_id>,
        std::set<server_id>>> partition;

    explicit cluster_sim(std::vector<server_id> ids) {
        std::set<server_id> all(ids.begin(), ids.end());
        for (auto id : ids) {
            std::set<server_id> peers;
            for (auto p : all)
                if (p != id)
                    peers.insert(p);
            nodes[id] = std::make_unique<
                server<sim_transport>>(id, peers, transport);
        }
    }

    // deliver one round of pending messages
    void step() {
        transport.deliver([&](const message& m) {
            if (crashed.count(m.to))
                return;
            if (is_partitioned(m.from, m.to))
                return;
            auto it = nodes.find(m.to);
            if (it != nodes.end())
                it->second->receive(m);
        });
    }

    void run(int n) {
        for (int i = 0; i < n; ++i)
            step();
    }

    void crash(server_id id) { crashed.insert(id); }

    void recover(server_id id) {
        crashed.erase(id);
        nodes.at(id)->restart();
    }

    void partition_cluster(
        std::set<server_id> a, std::set<server_id> b)
    {
        partition = {std::move(a), std::move(b)};
    }

    void heal() { partition.reset(); }

    server_id leader() const {
        for (auto& [id, s] : nodes)
            if (!crashed.count(id) &&
                s->state() == server_state::leader)
                return id;
        return 0;
    }

    std::vector<server_id> leaders() const {
        std::vector<server_id> out;
        for (auto& [id, s] : nodes)
            if (!crashed.count(id) &&
                s->state() == server_state::leader)
                out.push_back(id);
        return out;
    }

    // drive an election until a leader emerges or
    // max_rounds exhausted.  re-sends RV every round;
    // the not_yet_asked guard in the server skips peers
    // that have already responded, so votes accumulate
    // across rounds without duplicate sends.  restarts
    // the election with a new term only when all peers
    // have responded but quorum wasn't reached.
    server_id elect_leader(int max_rounds = 30) {
        server_id cand_id = 0;
        for (auto& [id, s] : nodes) {
            if (!crashed.count(id)) {
                s->timeout();
                cand_id = id;
                break;
            }
        }
        if (!cand_id)
            return 0;

        for (int r = 0; r < max_rounds; ++r) {
            auto& cand = nodes.at(cand_id);
            // re-send to non-responded peers every round
            for (auto& [pid, s] : nodes)
                if (pid != cand_id && !crashed.count(pid))
                    cand->request_vote(pid);
            step();
            cand->become_leader();
            if (cand->state() == server_state::leader)
                return cand_id;
            // restart election if all peers replied but
            // we still lack quorum (e.g. split votes)
            if (cand->votes_responded().size() >=
                cand->peers().size())
                cand->timeout();
        }
        return 0;
    }

    bool submit(const std::string& v) {
        server_id lid = leader();
        if (!lid)
            return false;
        nodes.at(lid)->client_request(v);
        return true;
    }

    // send AppendEntries from leader to all live peers
    void broadcast() {
        server_id lid = leader();
        if (!lid)
            return;
        for (auto& [pid, _] : nodes)
            if (pid != lid && !crashed.count(pid))
                nodes.at(lid)->append_entries(pid);
    }

    void advance() {
        server_id lid = leader();
        if (lid)
            nodes.at(lid)->advance_commit_index();
    }

    bool all_committed(index_t idx) const {
        for (auto& [id, s] : nodes)
            if (!crashed.count(id) && s->commit_index() < idx)
                return false;
        return true;
    }

    size_t live_count() const {
        return nodes.size() - crashed.size();
    }

  private:
    bool is_partitioned(server_id a, server_id b) const {
        if (!partition)
            return false;
        auto& [pa, pb] = *partition;
        return (pa.count(a) && pb.count(b)) ||
               (pb.count(a) && pa.count(b));
    }
};

} // namespace raftpp
