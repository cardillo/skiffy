#pragma once
#include <chrono>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <utility>
#include <vector>

#include "skiffy.hpp"

namespace skiffy {

// -------------------------------------------------------
// memory_transport
// -------------------------------------------------------

struct memory_transport {
    std::vector<message> sent;

    explicit memory_transport(skiffy::node_id = {}) {}

    void send(const message& m) { sent.push_back(m); }
    void clear() { sent.clear(); }
    void on_message(std::function<void(const skiffy::message&)>) {}
    void listen(uint16_t) {}
    void run() {}
    void stop() {}
    void post(std::function<void()> fn) { fn(); }
    template <typename Rep, typename Period>
    void schedule(std::chrono::duration<Rep, Period>, std::function<void()>) {
    }

    template <typename Fn>
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

    explicit sim_transport(skiffy::node_id = {}) {}

    void send(const message& m) { queue_.push_back(m); }
    void on_message(std::function<void(const skiffy::message&)>) {}
    void listen(uint16_t) {}
    void run() {}
    void stop() {}
    void post(std::function<void()> fn) { fn(); }
    template <typename Rep, typename Period>
    void schedule(std::chrono::duration<Rep, Period>, std::function<void()>) {
    }

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
// test_server
// -------------------------------------------------------
// Thin subclass of server that re-exposes private state
// accessors as public for use in unit tests.

namespace detail {

template <typename LogStore>
struct test_server_storage {
    LogStore log_storage_;
};

template <typename Transport, typename LogStore = memory_log_store>
struct test_server : test_server_storage<LogStore>,
                     server<Transport, LogStore> {
    using storage = test_server_storage<LogStore>;
    using base = server<Transport, LogStore>;

    test_server(node_id id, std::set<node_id> peers, Transport& t)
        : storage{}, base(id, std::move(peers), t, storage::log_storage_) {}

    term_t current_term() const { return base::current_term_; }
    node_id voted_for() const { return base::voted_for_; }
    index_t commit_index() const { return base::commit_index_; }
    std::vector<log_entry> log() const {
        std::vector<log_entry> v;
        for (size_t i = 0; i < base::log_store_.size(); ++i)
            v.push_back(base::log_store_[i]);
        return v;
    }
    const std::set<node_id>& votes_responded() const {
        return base::votes_responded_;
    }
    index_t next_index_for(node_id j) const {
        return base::next_index_.at(j);
    }
    index_t match_index_for(node_id j) const {
        return base::match_index_.at(j);
    }
    const std::optional<std::set<node_id>>& joint_config() const {
        return base::joint_config_;
    }
    term_t last_term() const { return base::last_term(); }
};

template <typename Transport>
test_server(node_id, std::set<node_id>, Transport&) -> test_server<Transport>;

} // namespace detail

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
    std::map<node_id, std::unique_ptr<detail::test_server<sim_transport>>>
        nodes;

    // nodes that have been crashed (drop inbound messages)
    std::set<node_id> crashed;

    // optional partition: messages between the two sets
    // are silently dropped in both directions
    std::optional<std::pair<std::set<node_id>, std::set<node_id>>> partition;

    explicit cluster_sim(std::vector<node_id> ids) {
        std::set<node_id> all(ids.begin(), ids.end());
        for (auto id : ids) {
            std::set<node_id> peers;
            for (auto p : all)
                if (p != id)
                    peers.insert(p);
            nodes[id] = std::make_unique<detail::test_server<sim_transport>>(
                id, peers, transport);
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

    void crash(node_id id) { crashed.insert(id); }

    void recover(node_id id) {
        crashed.erase(id);
        nodes.at(id)->restart();
    }

    void partition_cluster(std::set<node_id> a, std::set<node_id> b) {
        partition = {std::move(a), std::move(b)};
    }

    void heal() { partition.reset(); }

    node_id leader() const {
        for (auto& [id, s] : nodes)
            if (!crashed.count(id) &&
                s->state() == detail::server_state::leader)
                return id;
        return nil_id;
    }

    std::vector<node_id> leaders() const {
        std::vector<node_id> out;
        for (auto& [id, s] : nodes)
            if (!crashed.count(id) &&
                s->state() == detail::server_state::leader)
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
    node_id elect_leader(int max_rounds = 30) {
        node_id cand_id;
        for (auto& [id, s] : nodes) {
            if (!crashed.count(id)) {
                s->timeout();
                cand_id = id;
                break;
            }
        }
        if (cand_id.is_nil())
            return nil_id;

        for (int r = 0; r < max_rounds; ++r) {
            auto& cand = nodes.at(cand_id);
            // re-send to non-responded peers every round
            for (auto& [pid, s] : nodes)
                if (pid != cand_id && !crashed.count(pid))
                    cand->request_vote(pid);
            step();
            cand->become_leader();
            if (cand->state() == detail::server_state::leader)
                return cand_id;
            // restart election if all peers replied but
            // we still lack quorum (e.g. split votes)
            if (cand->votes_responded().size() >= cand->peers().size())
                cand->timeout();
        }
        return nil_id;
    }

    bool submit(const std::string& v) {
        node_id lid = leader();
        if (lid.is_nil())
            return false;
        nodes.at(lid)->client_request(v);
        return true;
    }

    // send AppendEntries from leader to all live peers
    void broadcast() {
        node_id lid = leader();
        if (lid.is_nil())
            return;
        for (auto& [pid, _] : nodes)
            if (pid != lid && !crashed.count(pid))
                nodes.at(lid)->append_entries(pid);
    }

    void advance() {
        node_id lid = leader();
        if (!lid.is_nil())
            nodes.at(lid)->advance_commit_index();
    }

    bool all_committed(index_t idx) const {
        for (auto& [id, s] : nodes)
            if (!crashed.count(id) && s->commit_index() < idx)
                return false;
        return true;
    }

    size_t live_count() const { return nodes.size() - crashed.size(); }

  private:
    bool is_partitioned(node_id a, node_id b) const {
        if (!partition)
            return false;
        auto& [pa, pb] = *partition;
        return (pa.count(a) && pb.count(b)) || (pb.count(a) && pa.count(b));
    }
};

} // namespace skiffy

// Loopback node_ids for use in all test files.
// Ports 1-5 are reserved but safe as non-routable
// test identifiers.
inline const skiffy::node_id s1({127, 0, 0, 1}, 1);
inline const skiffy::node_id s2({127, 0, 0, 2}, 2);
inline const skiffy::node_id s3(std::array<uint8_t, 16>{1}, 3);
inline const skiffy::node_id s4({127, 0, 0, 1}, 4);
inline const skiffy::node_id s5({127, 0, 0, 1}, 5);

// Drive s (initially a follower in a 3-node cluster with
// peers s2/s3) through an election to become leader, then
// clear any messages left in t.
inline void
make_leader(skiffy::detail::test_server<skiffy::memory_transport>& s,
            skiffy::memory_transport& t) {
    s.timeout();
    skiffy::message v;
    v.type = skiffy::msg_type::request_vote_resp;
    v.term = s.current_term();
    v.vote_granted = true;
    v.to = s.id();
    v.from = s2;
    s.receive(v);
    v.from = s3;
    s.receive(v);
    s.become_leader();
    t.clear();
}
