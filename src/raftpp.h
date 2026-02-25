#ifndef RAFTPP_H
#define RAFTPP_H

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "boost/sml.hpp"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

#include "asio.hpp"
#include "msgpack.hpp"

namespace raftpp {

// --- types ---

using server_id = uint64_t;
using term_t = uint64_t;
using index_t = uint64_t;

constexpr server_id nil_id = 0;

// --- logging ---

inline std::shared_ptr<spdlog::logger> logger() {
    static std::once_flag flag;
    std::call_once(flag, [] {
        auto l = spdlog::stdout_color_mt("raftpp");
        l->set_level(spdlog::level::warn);
    });
    return spdlog::get("raftpp");
}

// --- core enums ---

enum class entry_type : uint8_t {
    data = 0,
    config_joint = 1,
    config_final = 2,
};

enum class server_state { follower, candidate, leader };

enum class msg_type {
    request_vote_req,
    request_vote_resp,
    append_entries_req,
    append_entries_resp,
    install_snapshot_req,
    install_snapshot_resp,
    client_req,
};

// forward declaration for event structs below
struct message;

// --- SML events ---

struct evt_timeout {};
struct evt_restart {};
struct evt_become_leader {};
struct evt_advance {};

struct evt_higher_term {
    term_t term;
};

struct evt_rv_to {
    server_id j;
};
struct evt_ae_to {
    server_id j;
};

struct evt_client_req {
    std::string v;
};
struct evt_config_req {
    std::set<server_id> peers;
};

// inbound message events (pointer into
// a message that is alive for the call)
struct evt_rv_req {
    const message* m;
};
struct evt_rv_resp {
    const message* m;
};
struct evt_ae_req {
    const message* m;
};
struct evt_ae_resp {
    const message* m;
};
struct evt_snap_req {
    const message* m;
};
struct evt_snap_resp {
    const message* m;
};

// --- core structs ---

struct log_entry {
    term_t term = 0;
    entry_type type = entry_type::data;
    std::string value;

    bool operator==(const log_entry& o) const {
        return term == o.term && type == o.type && value == o.value;
    }
    bool operator!=(const log_entry& o) const { return !(*this == o); }

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        pk.pack_array(3);
        pk.pack(term);
        pk.pack(static_cast<uint8_t>(type));
        pk.pack(value);
    }
    void msgpack_unpack(msgpack::object const& o) {
        auto& a = o.via.array;
        term = a.ptr[0].as<term_t>();
        type = static_cast<entry_type>(a.ptr[1].as<uint8_t>());
        value = a.ptr[2].as<std::string>();
    }
};

struct snapshot_t {
    index_t index = 0;
    term_t term = 0;
    std::string data;

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        pk.pack_array(3);
        pk.pack(index);
        pk.pack(term);
        pk.pack(data);
    }
    void msgpack_unpack(msgpack::object const& o) {
        auto& a = o.via.array;
        index = a.ptr[0].as<index_t>();
        term = a.ptr[1].as<term_t>();
        data = a.ptr[2].as<std::string>();
    }
};

struct message {
    msg_type type;
    term_t term = 0;
    server_id from = 0;
    server_id to = 0;

    // request_vote_req fields
    std::optional<term_t> last_log_term;
    std::optional<index_t> last_log_index;

    // request_vote_resp fields
    std::optional<bool> vote_granted;

    // append_entries_req fields
    std::optional<index_t> prev_log_index;
    std::optional<term_t> prev_log_term;
    std::optional<std::vector<log_entry>> entries;
    std::optional<index_t> commit_index;

    // append_entries_resp fields
    std::optional<bool> success;
    std::optional<index_t> match_index;

    // install_snapshot fields
    std::optional<index_t> snapshot_index;
    std::optional<term_t> snapshot_term;
    std::optional<std::string> snapshot_data;

    // client_req fields
    std::optional<std::string> client_data;

    bool operator==(const message& o) const {
        return type == o.type && term == o.term && from == o.from &&
            to == o.to && last_log_term == o.last_log_term &&
            last_log_index == o.last_log_index &&
            vote_granted == o.vote_granted &&
            prev_log_index == o.prev_log_index &&
            prev_log_term == o.prev_log_term && entries == o.entries &&
            commit_index == o.commit_index && success == o.success &&
            match_index == o.match_index &&
            snapshot_index == o.snapshot_index &&
            snapshot_term == o.snapshot_term &&
            snapshot_data == o.snapshot_data && client_data == o.client_data;
    }
    bool operator!=(const message& o) const { return !(*this == o); }

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        auto p = [&pk](const auto& v) {
            if (v.has_value())
                pk.pack(*v);
            else
                pk.pack_nil();
        };
        pk.pack_array(17);
        pk.pack(static_cast<uint8_t>(type));
        pk.pack(term);
        pk.pack(from);
        pk.pack(to);
        p(last_log_term);
        p(last_log_index);
        p(vote_granted);
        p(prev_log_index);
        p(prev_log_term);
        p(entries);
        p(commit_index);
        p(success);
        p(match_index);
        p(snapshot_index);
        p(snapshot_term);
        p(snapshot_data);
        p(client_data);
    }
    void msgpack_unpack(msgpack::object const& o) {
        auto& a = o.via.array;
        auto nil = msgpack::type::NIL;
        type = static_cast<msg_type>(a.ptr[0].as<uint8_t>());
        term = a.ptr[1].as<term_t>();
        from = a.ptr[2].as<server_id>();
        to = a.ptr[3].as<server_id>();
        if (a.ptr[4].type == nil)
            last_log_term = {};
        else
            last_log_term = a.ptr[4].as<term_t>();
        if (a.ptr[5].type == nil)
            last_log_index = {};
        else
            last_log_index = a.ptr[5].as<index_t>();
        if (a.ptr[6].type == nil)
            vote_granted = {};
        else
            vote_granted = a.ptr[6].as<bool>();
        if (a.ptr[7].type == nil)
            prev_log_index = {};
        else
            prev_log_index = a.ptr[7].as<index_t>();
        if (a.ptr[8].type == nil)
            prev_log_term = {};
        else
            prev_log_term = a.ptr[8].as<term_t>();
        if (a.ptr[9].type == nil) {
            entries = {};
        } else {
            std::vector<log_entry> v;
            a.ptr[9].convert(v);
            entries = std::move(v);
        }
        if (a.ptr[10].type == nil)
            commit_index = {};
        else
            commit_index = a.ptr[10].as<index_t>();
        if (a.ptr[11].type == nil)
            success = {};
        else
            success = a.ptr[11].as<bool>();
        if (a.ptr[12].type == nil)
            match_index = {};
        else
            match_index = a.ptr[12].as<index_t>();
        if (a.ptr[13].type == nil)
            snapshot_index = {};
        else
            snapshot_index = a.ptr[13].as<index_t>();
        if (a.ptr[14].type == nil)
            snapshot_term = {};
        else
            snapshot_term = a.ptr[14].as<term_t>();
        if (a.ptr[15].type == nil)
            snapshot_data = {};
        else
            snapshot_data = a.ptr[15].as<std::string>();
        if (a.ptr[16].type == nil)
            client_data = {};
        else
            client_data = a.ptr[16].as<std::string>();
    }
};

// --- transport concept ---
//
// Transport must provide:
//   void send(const raftpp::message& m);

// -------------------------------------------------------
// StateMachine concept
//
// StateMachine must provide:
//   void apply(const log_entry&)
//   std::string snapshot() const
//   void install(const std::string& data)
// -------------------------------------------------------

struct log_state_machine {
    std::vector<log_entry> applied;

    void apply(const log_entry& e) { applied.push_back(e); }

    std::string snapshot() const {
        msgpack::sbuffer buf;
        msgpack::pack(buf, applied);
        return {buf.data(), buf.data() + buf.size()};
    }

    void install(const std::string& data) {
        msgpack::object_handle oh = msgpack::unpack(data.data(), data.size());
        oh.get().convert(applied);
    }
};

// -------------------------------------------------------
// LogStore concept
//
// LogStore must provide:
//   void append(const log_entry&)
//   void truncate(size_t n)
//   void clear()
//   size_t size() const
//   bool empty() const
//   const log_entry& back() const
//   const log_entry& operator[](size_t) const
//   log_entry& operator[](size_t)
//   const std::vector<log_entry>& entries() const
// -------------------------------------------------------

struct memory_log_store {
    void append(const log_entry& e) { entries_.push_back(e); }
    void truncate(size_t n) { entries_.resize(n); }
    void clear() { entries_.clear(); }
    size_t size() const { return entries_.size(); }
    bool empty() const { return entries_.empty(); }

    const log_entry& back() const { return entries_.back(); }
    const log_entry& operator[](size_t i) const { return entries_[i]; }
    log_entry& operator[](size_t i) { return entries_[i]; }
    const std::vector<log_entry>& entries() const { return entries_; }

  private:
    std::vector<log_entry> entries_;
};

struct file_log_store {
    file_log_store() = default;
    explicit file_log_store(const std::string& path_prefix)
        : wal_path_(path_prefix + ".wal"), snap_path_(path_prefix + ".snap") {
    }

    void load() {
        std::ifstream f(wal_path_, std::ios::binary);
        if (!f)
            return;
        std::string data(std::istreambuf_iterator<char>(f), {});
        if (data.empty())
            return;
        msgpack::unpacker unp;
        unp.reserve_buffer(data.size());
        std::copy(data.begin(), data.end(), unp.buffer());
        unp.buffer_consumed(data.size());
        msgpack::object_handle oh;
        while (unp.next(oh)) {
            log_entry e;
            oh.get().convert(e);
            entries_.push_back(e);
        }
    }

    void append(const log_entry& e) {
        entries_.push_back(e);
        std::ofstream f(wal_path_, std::ios::binary | std::ios::app);
        msgpack::sbuffer buf;
        msgpack::pack(buf, e);
        f.write(buf.data(), buf.size());
    }

    void truncate(size_t n) {
        entries_.resize(n);
        std::ofstream f(wal_path_, std::ios::binary | std::ios::trunc);
        for (auto& e : entries_) {
            msgpack::sbuffer buf;
            msgpack::pack(buf, e);
            f.write(buf.data(), buf.size());
        }
    }

    void clear() {
        entries_.clear();
        std::ofstream f(wal_path_, std::ios::binary | std::ios::trunc);
    }

    size_t size() const { return entries_.size(); }
    bool empty() const { return entries_.empty(); }

    const log_entry& back() const { return entries_.back(); }
    const log_entry& operator[](size_t i) const { return entries_[i]; }
    log_entry& operator[](size_t i) { return entries_[i]; }
    const std::vector<log_entry>& entries() const { return entries_; }

    void save_snapshot(const snapshot_t& s) {
        std::ofstream f(snap_path_, std::ios::binary | std::ios::trunc);
        msgpack::sbuffer buf;
        msgpack::pack(buf, s);
        f.write(buf.data(), buf.size());
    }

    std::optional<snapshot_t> load_snapshot() {
        std::ifstream f(snap_path_, std::ios::binary);
        if (!f)
            return std::nullopt;
        std::string data(std::istreambuf_iterator<char>(f), {});
        if (data.empty())
            return std::nullopt;
        msgpack::object_handle oh = msgpack::unpack(data.data(), data.size());
        snapshot_t s;
        oh.get().convert(s);
        return s;
    }

  private:
    std::vector<log_entry> entries_;
    std::string wal_path_;
    std::string snap_path_;
};

// --- memory_transport ---

struct memory_transport {
    std::vector<message> sent;

    void send(const message& m) { sent.push_back(m); }
    void clear() { sent.clear(); }
};

// --- server ---

template <typename Transport, typename LogStore = memory_log_store,
          typename StateMachine = log_state_machine>
class server {
    // SML state tags (internal)
    struct s_follower {};
    struct s_candidate {};
    struct s_leader {};

  public:
    // 3-arg: uses owned LogStore + StateMachine
    server(server_id self, std::set<server_id> peers, Transport& transport)
        : log_store_own_{}, state_machine_own_{}, log_store_(log_store_own_),
          state_machine_(state_machine_own_), transport_(transport),
          id_(self), peers_(std::move(peers)), sm_{raft_sm{this}} {
        init_state();
    }

    // 5-arg: external LogStore + StateMachine
    server(server_id self, std::set<server_id> peers, Transport& transport,
           LogStore& ls, StateMachine& sm)
        : log_store_own_{}, state_machine_own_{}, log_store_(ls),
          state_machine_(sm), transport_(transport), id_(self),
          peers_(std::move(peers)), sm_{raft_sm{this}} {
        init_state();
    }

    // --- TLA+ actions (public API) ---

    void restart() { sm_.process_event(evt_restart{}); }

    void timeout() { sm_.process_event(evt_timeout{}); }

    void request_vote(server_id j) { sm_.process_event(evt_rv_to{j}); }

    void become_leader() { sm_.process_event(evt_become_leader{}); }

    void client_request(std::string v) {
        sm_.process_event(evt_client_req{std::move(v)});
    }

    // config_request: leader initiates membership
    // change via joint consensus
    void config_request(std::set<server_id> new_peers) {
        sm_.process_event(evt_config_req{std::move(new_peers)});
    }

    void append_entries(server_id j) { sm_.process_event(evt_ae_to{j}); }

    void advance_commit_index() { sm_.process_event(evt_advance{}); }

    void set_compact_threshold(size_t n) { compact_threshold_ = n; }

    void set_on_peer_removed(std::function<void(server_id)> cb) {
        on_peer_removed_ = std::move(cb);
    }

    void set_on_peer_added(std::function<void(server_id)> cb) {
        on_peer_added_ = std::move(cb);
    }

    void set_on_compact(std::function<void()> cb) {
        on_compact_ = std::move(cb);
    }

    void compact() {
        if (commit_index_ <= snapshot_index_)
            return;
        index_t new_snap = commit_index_;
        term_t new_term = entry_term(new_snap);

        // collect entries after new_snap
        std::vector<log_entry> tail;
        size_t keep_from = new_snap - snapshot_index_;
        for (size_t i = keep_from; i < log_store_.size(); ++i) {
            tail.push_back(log_store_[i]);
        }
        log_store_.clear();
        for (auto& e : tail)
            log_store_.append(e);

        snapshot_index_ = new_snap;
        snapshot_term_ = new_term;
        if (on_compact_)
            on_compact_();
    }

    void receive(const message& m) {
        if (m.to != id_) {
            logger()->warn("server {} recv msg for {},"
                           " dropping",
                           id_, m.to);
            return;
        }

        logger()->debug("server {} recv mtype={} from {}", id_,
                        static_cast<int>(m.type), m.from);

        if (m.term > current_term_) {
            sm_.process_event(evt_higher_term{m.term});
        }

        switch (m.type) {
            case msg_type::request_vote_req:
                sm_.process_event(evt_rv_req{&m});
                break;
            case msg_type::request_vote_resp:
                if (!drop_stale_response(m)) {
                    sm_.process_event(evt_rv_resp{&m});
                } else {
                    logger()->warn("server {} stale rv_resp"
                                   " from {}",
                                   id_, m.from);
                }
                break;
            case msg_type::append_entries_req:
                sm_.process_event(evt_ae_req{&m});
                break;
            case msg_type::append_entries_resp:
                if (!drop_stale_response(m)) {
                    sm_.process_event(evt_ae_resp{&m});
                } else {
                    logger()->warn("server {} stale ae_resp"
                                   " from {}",
                                   id_, m.from);
                }
                break;
            case msg_type::install_snapshot_req:
                sm_.process_event(evt_snap_req{&m});
                break;
            case msg_type::install_snapshot_resp:
                if (!drop_stale_response(m)) {
                    sm_.process_event(evt_snap_resp{&m});
                }
                break;
            case msg_type::client_req:
                break;
        }
    }

    void add_peer(server_id j) {
        peers_.insert(j);
        next_index_[j] = last_log_index() + 1;
        match_index_[j] = 0;
    }

    void remove_peer(server_id j) {
        peers_.erase(j);
        next_index_.erase(j);
        match_index_.erase(j);
    }

    // --- accessors for testing ---

    server_id id() const { return id_; }
    term_t current_term() const { return current_term_; }
    server_state state() const {
        if (sm_.is(boost::sml::state<s_leader>))
            return server_state::leader;
        if (sm_.is(boost::sml::state<s_candidate>))
            return server_state::candidate;
        return server_state::follower;
    }
    server_id voted_for() const { return voted_for_; }
    index_t commit_index() const { return commit_index_; }
    index_t snapshot_index() const { return snapshot_index_; }
    term_t snapshot_term() const { return snapshot_term_; }

    const std::vector<log_entry>& log() const { return log_store_.entries(); }
    const std::set<server_id>& votes_responded() const {
        return votes_responded_;
    }
    const std::set<server_id>& votes_granted() const {
        return votes_granted_;
    }
    index_t next_index_for(server_id j) const { return next_index_.at(j); }
    index_t match_index_for(server_id j) const { return match_index_.at(j); }
    const std::set<server_id>& peers() const { return peers_; }
    const std::optional<std::set<server_id>>& joint_config() const {
        return joint_config_;
    }
    const StateMachine& state_machine() const { return state_machine_; }

    term_t last_term() const {
        if (log_store_.empty())
            return snapshot_term_;
        return log_store_.back().term;
    }

    bool is_quorum(const std::set<server_id>& s) const {
        if (!joint_config_) {
            size_t cluster = peers_.size() + 1;
            return s.size() * 2 > cluster;
        }
        // joint consensus: majority in both configs
        auto count_in = [&](const std::set<server_id>& cfg) {
            size_t c = 0;
            for (auto sid : s)
                if (sid == id_ || cfg.count(sid))
                    ++c;
            return c;
        };
        size_t old_sz = peers_.size() + 1;
        size_t new_sz = joint_config_->size() + 1;
        return count_in(peers_) * 2 > old_sz &&
            count_in(*joint_config_) * 2 > new_sz;
    }

  private:
    void init_state() {
        current_term_ = 1;
        voted_for_ = nil_id;
        commit_index_ = 0;
        last_applied_ = 0;
        snapshot_index_ = 0;
        snapshot_term_ = 0;
        votes_responded_.clear();
        votes_granted_.clear();
        init_leader_vars();
        committed_peers_ = peers_;
    }

    void init_leader_vars() {
        for (auto& p : peers_) {
            next_index_[p] = 1;
            match_index_[p] = 0;
        }
        next_index_[id_] = 1;
        match_index_[id_] = 0;
    }

    index_t last_log_index() const {
        return snapshot_index_ + log_store_.size();
    }

    size_t log_offset(index_t idx) const { return idx - snapshot_index_ - 1; }

    term_t entry_term(index_t idx) const {
        return log_store_[log_offset(idx)].term;
    }

    void do_update_term_action(term_t new_term) {
        logger()->debug("server {} term {} -> {}", id_, current_term_,
                        new_term);
        current_term_ = new_term;
        voted_for_ = nil_id;
    }

    bool drop_stale_response(const message& m) const {
        return m.term < current_term_;
    }

    void handle_request_vote_request(const message& m) {
        server_id j = m.from;
        term_t m_last_log_term = m.last_log_term.value_or(0);
        index_t m_last_log_index = m.last_log_index.value_or(0);

        bool log_ok = m_last_log_term > last_term() ||
            (m_last_log_term == last_term() &&
             m_last_log_index >= last_log_index());

        bool grant = m.term == current_term_ && log_ok &&
            (voted_for_ == nil_id || voted_for_ == j);

        if (grant) {
            voted_for_ = j;
        }

        message resp;
        resp.type = msg_type::request_vote_resp;
        resp.term = current_term_;
        resp.vote_granted = grant;
        resp.from = id_;
        resp.to = j;
        transport_.send(resp);
    }

    void handle_request_vote_response(const message& m) {
        if (m.term != current_term_)
            return;

        server_id j = m.from;
        votes_responded_.insert(j);

        if (m.vote_granted.value_or(false)) {
            votes_granted_.insert(j);
        }
    }

    void handle_append_entries_request(const message& m) {
        server_id j = m.from;
        index_t prev_idx = m.prev_log_index.value_or(0);
        term_t prev_term = m.prev_log_term.value_or(0);
        const auto& entries = m.entries.value_or(std::vector<log_entry>{});
        index_t m_commit = m.commit_index.value_or(0);

        bool log_ok = prev_idx == 0 || prev_idx <= snapshot_index_ ||
            (prev_idx > snapshot_index_ && prev_idx <= last_log_index() &&
             log_store_[log_offset(prev_idx)].term == prev_term);

        // reject
        if (m.term < current_term_ ||
            (m.term == current_term_ && is_follower() && !log_ok)) {
            message resp;
            resp.type = msg_type::append_entries_resp;
            resp.term = current_term_;
            resp.success = false;
            resp.match_index = 0;
            resp.from = id_;
            resp.to = j;
            transport_.send(resp);
            return;
        }

        // accept: apply each entry in order
        for (size_t i = 0; i < entries.size(); ++i) {
            index_t idx = prev_idx + 1 + static_cast<index_t>(i);
            if (idx <= snapshot_index_)
                continue;
            size_t si = log_offset(idx);
            if (si < log_store_.size()) {
                if (log_store_[si].term != entries[i].term) {
                    revert_config_if_needed(si);
                    log_store_.truncate(si);
                    log_store_.append(entries[i]);
                }
            } else {
                log_store_.append(entries[i]);
            }
        }

        commit_index_ = m_commit;
        apply_committed();

        message resp;
        resp.type = msg_type::append_entries_resp;
        resp.term = current_term_;
        resp.success = true;
        resp.match_index = prev_idx + static_cast<index_t>(entries.size());
        resp.from = id_;
        resp.to = j;
        transport_.send(resp);
    }

    void handle_append_entries_response(const message& m) {
        if (m.term != current_term_)
            return;

        server_id j = m.from;
        if (m.success.value_or(false)) {
            index_t mi = m.match_index.value_or(0);
            next_index_[j] = mi + 1;
            match_index_[j] = mi;
        } else {
            if (next_index_[j] > 1) {
                next_index_[j]--;
            }
        }
    }

    void handle_install_snapshot_request(const message& m) {
        if (m.term < current_term_)
            return;
        index_t si = m.snapshot_index.value_or(0);
        term_t st = m.snapshot_term.value_or(0);
        if (snapshot_index_ >= si)
            return;

        state_machine_.install(m.snapshot_data.value_or(""));
        snapshot_index_ = si;
        snapshot_term_ = st;
        last_applied_ = snapshot_index_;
        commit_index_ = snapshot_index_;
        log_store_.clear();

        message resp;
        resp.type = msg_type::install_snapshot_resp;
        resp.term = current_term_;
        resp.snapshot_index = si;
        resp.from = id_;
        resp.to = m.from;
        transport_.send(resp);
    }

    void handle_install_snapshot_response(const message& m) {
        if (m.term != current_term_)
            return;
        server_id j = m.from;
        index_t si = m.snapshot_index.value_or(0);
        next_index_[j] = si + 1;
        match_index_[j] = si;
    }

    bool log_has_config_final() const {
        for (size_t i = 0; i < log_store_.size(); ++i) {
            if (log_store_[i].type == entry_type::config_final)
                return true;
        }
        return false;
    }

    void ensure_config_final() {
        if (log_has_config_final())
            return;
        msgpack::sbuffer buf;
        msgpack::pack(buf, *joint_config_);
        log_entry ce;
        ce.term = current_term_;
        ce.type = entry_type::config_final;
        ce.value = {buf.data(), buf.data() + buf.size()};
        log_store_.append(ce);
    }

    void apply_config_joint(const log_entry& e) {
        std::set<server_id> np;
        msgpack::object_handle oh =
            msgpack::unpack(e.value.data(), e.value.size());
        oh.get().convert(np);
        joint_config_ = np;
        if (is_leader())
            ensure_config_final();
    }

    void apply_config_final(const log_entry& e) {
        std::set<server_id> np;
        msgpack::object_handle oh =
            msgpack::unpack(e.value.data(), e.value.size());
        oh.get().convert(np);
        for (auto p : np) {
            if (p != id_ && !peers_.count(p)) {
                add_peer(p);
                if (on_peer_added_)
                    on_peer_added_(p);
            }
        }
        if (on_peer_removed_) {
            for (auto p : peers_)
                if (!np.count(p))
                    on_peer_removed_(p);
        }
        peers_ = np;
        peers_.erase(id_);
        committed_peers_ = peers_;
        joint_config_ = std::nullopt;
    }

    void revert_config_if_needed(size_t from_offset) {
        for (size_t i = from_offset; i < log_store_.size(); ++i) {
            index_t idx = snapshot_index_ + 1 + static_cast<index_t>(i);
            if (idx <= last_applied_)
                continue;
            if (log_store_[i].type == entry_type::config_joint) {
                peers_ = committed_peers_;
                joint_config_ = std::nullopt;
                return;
            }
        }
    }

    void apply_committed() {
        for (index_t idx = last_applied_ + 1; idx <= commit_index_; ++idx) {
            if (idx <= snapshot_index_)
                continue;
            auto& e = log_store_[log_offset(idx)];
            if (e.type == entry_type::data)
                state_machine_.apply(e);
            else if (e.type == entry_type::config_joint)
                apply_config_joint(e);
            else if (e.type == entry_type::config_final)
                apply_config_final(e);
        }
        last_applied_ = commit_index_;
        if (compact_threshold_ > 0 && log_store_.size() >= compact_threshold_)
            compact();
    }

    // --- state helpers ---

    bool is_follower() const { return sm_.is(boost::sml::state<s_follower>); }
    bool is_leader() const { return sm_.is(boost::sml::state<s_leader>); }

    // --- SML action methods ---

    void do_timeout_action() {
        current_term_++;
        voted_for_ = id_;
        votes_responded_.clear();
        votes_granted_.clear();
        votes_granted_.insert(id_);
        logger()->info("server {} timeout, starting"
                       " election term {}",
                       id_, current_term_);
    }

    void do_restart_action() {
        votes_responded_.clear();
        votes_granted_.clear();
        init_leader_vars();
        commit_index_ = 0;
        // currentTerm, votedFor, log preserved
    }

    void do_become_leader_action() {
        index_t next = last_log_index() + 1;
        for (auto& p : peers_) {
            next_index_[p] = next;
            match_index_[p] = 0;
        }
        next_index_[id_] = next;
        match_index_[id_] = 0;
        if (joint_config_.has_value())
            ensure_config_final();
        logger()->info("server {} became leader term {}", id_, current_term_);
    }

    void do_send_rv_req_action(server_id j) {
        logger()->debug("server {} send request_vote"
                        " to {}",
                        id_, j);
        message m;
        m.type = msg_type::request_vote_req;
        m.term = current_term_;
        m.last_log_term = last_term();
        m.last_log_index = last_log_index();
        m.from = id_;
        m.to = j;
        transport_.send(m);
    }

    void do_client_request_action(const std::string& v) {
        log_entry entry;
        entry.term = current_term_;
        entry.type = entry_type::data;
        entry.value = v;
        log_store_.append(std::move(entry));
    }

    void do_config_request_action(const std::set<server_id>& new_peers) {
        msgpack::sbuffer buf;
        msgpack::pack(buf, new_peers);
        std::string encoded(buf.data(), buf.data() + buf.size());
        log_entry entry;
        entry.term = current_term_;
        entry.type = entry_type::config_joint;
        entry.value = std::move(encoded);
        log_store_.append(std::move(entry));
        joint_config_ = new_peers;
        for (auto p : new_peers) {
            if (!peers_.count(p))
                add_peer(p);
        }
    }

    void do_append_entries_action(server_id j) {
        if (j == id_)
            return;

        // send InstallSnapshot if follower lags
        // behind our snapshot
        if (next_index_[j] <= snapshot_index_) {
            message m;
            m.type = msg_type::install_snapshot_req;
            m.term = current_term_;
            m.from = id_;
            m.to = j;
            m.snapshot_index = snapshot_index_;
            m.snapshot_term = snapshot_term_;
            m.snapshot_data = state_machine_.snapshot();
            transport_.send(m);
            return;
        }

        index_t prev_log_index = next_index_[j] - 1;
        term_t prev_log_term = 0;
        if (prev_log_index > snapshot_index_) {
            prev_log_term = entry_term(prev_log_index);
        } else if (prev_log_index == snapshot_index_ && snapshot_index_ > 0) {
            prev_log_term = snapshot_term_;
        }

        std::vector<log_entry> entries;
        for (index_t i = next_index_[j]; i <= last_log_index(); ++i) {
            entries.push_back(log_store_[log_offset(i)]);
        }

        index_t last_entry =
            prev_log_index + static_cast<index_t>(entries.size());
        index_t commit = std::min(commit_index_, last_entry);

        message m;
        m.type = msg_type::append_entries_req;
        m.term = current_term_;
        m.prev_log_index = prev_log_index;
        m.prev_log_term = prev_log_term;
        m.entries = std::move(entries);
        m.commit_index = commit;
        m.from = id_;
        m.to = j;
        transport_.send(m);
    }

    void do_advance_commit_action() {
        index_t new_commit = commit_index_;
        for (index_t idx = last_log_index(); idx > snapshot_index_; --idx) {
            if (entry_term(idx) != current_term_) {
                continue;
            }
            std::set<server_id> agree;
            agree.insert(id_);
            for (auto& p : peers_) {
                if (match_index_[p] >= idx) {
                    agree.insert(p);
                }
            }
            if (is_quorum(agree)) {
                new_commit = idx;
                break;
            }
        }
        commit_index_ = new_commit;
        apply_committed();
    }

    // --- SML state machine definition ---

    struct raft_sm {
        server* self_;
        explicit raft_sm(server* s) : self_(s) {}

        auto operator()() const noexcept {
            namespace sml = boost::sml;
            server* self = self_;

            auto fs = sml::state<s_follower>;
            auto cs = sml::state<s_candidate>;
            auto ls = sml::state<s_leader>;

            // guards
            auto has_quorum = [self]() noexcept {
                return self->is_quorum(self->votes_granted_);
            };
            auto same_term = [self](const evt_ae_req& e) noexcept {
                return e.m->term == self->current_term_;
            };
            auto not_yet_asked = [self](const evt_rv_to& e) noexcept {
                return !self->votes_responded_.count(e.j);
            };

            // actions
            auto do_start_election = [self]() noexcept {
                self->do_timeout_action();
            };
            auto do_restart = [self]() noexcept {
                self->do_restart_action();
            };
            auto do_update_term = [self](const evt_higher_term& e) noexcept {
                self->do_update_term_action(e.term);
            };
            auto do_init_leader = [self]() noexcept {
                self->do_become_leader_action();
            };
            auto do_send_rv = [self](const evt_rv_to& e) noexcept {
                self->do_send_rv_req_action(e.j);
            };
            auto do_rv_req = [self](const evt_rv_req& e) noexcept {
                self->handle_request_vote_request(*e.m);
            };
            auto do_rv_resp = [self](const evt_rv_resp& e) noexcept {
                self->handle_request_vote_response(*e.m);
            };
            auto do_ae_req = [self](const evt_ae_req& e) noexcept {
                self->handle_append_entries_request(*e.m);
            };
            auto do_ae_resp = [self](const evt_ae_resp& e) noexcept {
                self->handle_append_entries_response(*e.m);
            };
            auto do_snap_req = [self](const evt_snap_req& e) noexcept {
                self->handle_install_snapshot_request(*e.m);
            };
            auto do_snap_resp = [self](const evt_snap_resp& e) noexcept {
                self->handle_install_snapshot_response(*e.m);
            };
            auto do_client = [self](const evt_client_req& e) noexcept {
                self->do_client_request_action(e.v);
            };
            auto do_config = [self](const evt_config_req& e) noexcept {
                self->do_config_request_action(e.peers);
            };
            auto do_ae_to = [self](const evt_ae_to& e) noexcept {
                self->do_append_entries_action(e.j);
            };
            auto do_advance = [self]() noexcept {
                self->do_advance_commit_action();
            };

            // transition table
            return sml::make_transition_table(
                // follower
                *fs + sml::event<evt_timeout> / do_start_election = cs,

                fs + sml::event<evt_restart> / do_restart = fs,

                fs + sml::event<evt_higher_term> / do_update_term = fs,

                fs + sml::event<evt_rv_req> / do_rv_req = fs,

                fs + sml::event<evt_ae_req> / do_ae_req = fs,

                fs + sml::event<evt_snap_req> / do_snap_req = fs,

                // candidate
                cs + sml::event<evt_timeout> / do_start_election = cs,

                cs + sml::event<evt_restart> / do_restart = fs,

                cs + sml::event<evt_higher_term> / do_update_term = fs,

                cs +
                    sml::event<evt_become_leader>[has_quorum] /
                        do_init_leader = ls,

                cs + sml::event<evt_rv_to>[not_yet_asked] / do_send_rv = cs,

                cs + sml::event<evt_rv_req> / do_rv_req = cs,

                cs + sml::event<evt_rv_resp> / do_rv_resp = cs,

                // candidate receiving same-term
                // AppendEntries steps down without
                // processing the message
                cs + sml::event<evt_ae_req>[same_term] = fs,

                cs + sml::event<evt_snap_req> / do_snap_req = cs,

                // leader
                ls + sml::event<evt_restart> / do_restart = fs,

                ls + sml::event<evt_higher_term> / do_update_term = fs,

                ls + sml::event<evt_rv_req> / do_rv_req = ls,

                ls + sml::event<evt_ae_resp> / do_ae_resp = ls,

                ls + sml::event<evt_snap_resp> / do_snap_resp = ls,

                ls + sml::event<evt_ae_req> / do_ae_req = ls,

                ls + sml::event<evt_client_req> / do_client = ls,

                ls + sml::event<evt_config_req> / do_config = ls,

                ls + sml::event<evt_ae_to> / do_ae_to = ls,

                ls + sml::event<evt_advance> / do_advance = ls);
        }
    };

    // --- owned instances (3-arg constructor) ---
    LogStore log_store_own_;
    StateMachine state_machine_own_;

    // --- references (may point to own or external) ---
    LogStore& log_store_;
    StateMachine& state_machine_;
    Transport& transport_;

    // --- identity ---
    server_id id_;
    std::set<server_id> peers_;

    // serverVars
    term_t current_term_ = 1;
    server_id voted_for_ = nil_id;

    // logVars
    index_t commit_index_ = 0;

    // snapshot state
    index_t snapshot_index_ = 0;
    term_t snapshot_term_ = 0;
    index_t last_applied_ = 0;
    size_t compact_threshold_ = 0;

    // membership
    std::optional<std::set<server_id>> joint_config_;
    std::set<server_id> committed_peers_;
    std::function<void(server_id)> on_peer_removed_;
    std::function<void(server_id)> on_peer_added_;
    std::function<void()> on_compact_;

    // candidateVars
    std::set<server_id> votes_responded_;
    std::set<server_id> votes_granted_;

    // leaderVars
    std::map<server_id, index_t> next_index_;
    std::map<server_id, index_t> match_index_;

    // SM must be last: raft_sm must be complete
    mutable boost::sml::sm<raft_sm> sm_;
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
// asio_transport
// -------------------------------------------------------

class asio_transport {
  public:
    using endpoint_t = asio::ip::tcp::endpoint;
    using callback_t = std::function<void(const message&)>;

    asio_transport(server_id self, asio::io_context& io)
        : self_(self), io_(io), acceptor_(io) {}

    // Must be called before run() — not thread-safe.
    void set_callback(callback_t cb) { on_message_ = std::move(cb); }

    void add_peer(server_id id, const endpoint_t& ep) {
        peers_[id] = ep;
        logger()->debug("transport {} add_peer {} at {}:{}", self_, id,
                        ep.address().to_string(), ep.port());
    }

    void remove_peer(server_id id) { peers_.erase(id); }

    void listen(const endpoint_t& ep) {
        acceptor_.open(ep.protocol());
        acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true));
        acceptor_.bind(ep);
        acceptor_.listen();
        do_accept();
    }

    void send(const message& m) {
        auto it = peers_.find(m.to);
        if (it == peers_.end())
            return;
        logger()->debug("transport {} send mtype={} to {}", self_,
                        static_cast<int>(m.type), m.to);
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, m);
        auto buf = std::make_shared<std::vector<uint8_t>>(
            reinterpret_cast<const uint8_t*>(sbuf.data()),
            reinterpret_cast<const uint8_t*>(sbuf.data()) + sbuf.size());
        do_send(it->second, buf);
    }

  private:
    static constexpr size_t read_buf_size = 4096;

    void do_accept() {
        auto sock = std::make_shared<asio::ip::tcp::socket>(io_);
        acceptor_.async_accept(*sock, [this, sock](asio::error_code ec) {
            if (!ec) {
                logger()->debug("transport {} accepted"
                                " connection",
                                self_);
                do_read(sock);
            }
            do_accept();
        });
    }

    void do_read(const std::shared_ptr<asio::ip::tcp::socket>& sock) {
        auto unpacker = std::make_shared<msgpack::unpacker>();
        do_read_loop(sock, unpacker);
    }

    void do_read_loop(const std::shared_ptr<asio::ip::tcp::socket>& sock,
                      const std::shared_ptr<msgpack::unpacker>& unpacker) {
        unpacker->reserve_buffer(read_buf_size);
        sock->async_read_some(
            asio::buffer(unpacker->buffer(), read_buf_size),
            [this, sock, unpacker](asio::error_code ec, size_t n) {
                if (ec) {
                    logger()->debug("transport {} read"
                                    " done: {}",
                                    self_, ec.message());
                    return;
                }
                unpacker->buffer_consumed(n);
                msgpack::object_handle oh;
                while (unpacker->next(oh)) {
                    if (on_message_) {
                        message msg;
                        oh.get().convert(msg);
                        on_message_(msg);
                    }
                }
                do_read_loop(sock, unpacker);
            });
    }

    void do_send(const endpoint_t& ep,
                 const std::shared_ptr<std::vector<uint8_t>>& buf) {
        auto sock = std::make_shared<asio::ip::tcp::socket>(io_);
        sock->async_connect(ep, [this, sock, buf, ep](asio::error_code ec) {
            if (ec) {
                logger()->error("transport {} connect"
                                " error {}:{}: {}",
                                self_, ep.address().to_string(), ep.port(),
                                ec.message());
                return;
            }
            asio::async_write(*sock, asio::buffer(*buf),
                              [sock, buf](asio::error_code, size_t) {});
        });
    }

    server_id self_;
    asio::io_context& io_;
    asio::ip::tcp::acceptor acceptor_;
    std::map<server_id, endpoint_t> peers_;
    callback_t on_message_;
};

// -------------------------------------------------------
// membership
// -------------------------------------------------------

struct member_info {
    server_id id = 0;
    std::string host;
    uint16_t raft_port = 0;
    uint16_t mem_port = 0;

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        pk.pack_array(4);
        pk.pack(id);
        pk.pack(host);
        pk.pack(raft_port);
        pk.pack(mem_port);
    }

    void msgpack_unpack(msgpack::object const& o) {
        auto& a = o.via.array;
        id = a.ptr[0].as<server_id>();
        host = a.ptr[1].as<std::string>();
        raft_port = a.ptr[2].as<uint16_t>();
        mem_port = a.ptr[3].as<uint16_t>();
    }

    asio::ip::tcp::endpoint endpoint() const {
        return {asio::ip::make_address(host), raft_port};
    }
    asio::ip::tcp::endpoint mem_endpoint() const {
        return {asio::ip::make_address(host), mem_port};
    }
};

enum class mem_msg_type : uint8_t {
    join_req = 1,
    join_resp = 2,
    announce = 3,
    leave_req = 4,
    remove = 5,
};

struct mem_message {
    mem_msg_type type;
    std::optional<server_id> joiner_id;
    std::optional<std::string> joiner_host;
    std::optional<uint16_t> joiner_raft_port;
    std::optional<uint16_t> joiner_mem_port;
    std::optional<std::vector<member_info>> members;

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        auto p = [&pk](const auto& v) {
            if (v.has_value())
                pk.pack(*v);
            else
                pk.pack_nil();
        };
        pk.pack_array(6);
        pk.pack(static_cast<uint8_t>(type));
        p(joiner_id);
        p(joiner_host);
        p(joiner_raft_port);
        p(joiner_mem_port);
        p(members);
    }

    void msgpack_unpack(msgpack::object const& o) {
        auto& a = o.via.array;
        auto nil = msgpack::type::NIL;
        type = static_cast<mem_msg_type>(a.ptr[0].as<uint8_t>());
        if (a.ptr[1].type == nil)
            joiner_id = {};
        else
            joiner_id = a.ptr[1].as<server_id>();
        if (a.ptr[2].type == nil)
            joiner_host = {};
        else
            joiner_host = a.ptr[2].as<std::string>();
        if (a.ptr[3].type == nil)
            joiner_raft_port = {};
        else
            joiner_raft_port = a.ptr[3].as<uint16_t>();
        if (a.ptr[4].type == nil)
            joiner_mem_port = {};
        else
            joiner_mem_port = a.ptr[4].as<uint16_t>();
        if (a.ptr[5].type == nil) {
            members = {};
        } else {
            std::vector<member_info> v;
            a.ptr[5].convert(v);
            members = std::move(v);
        }
    }
};

// -------------------------------------------------------
// membership_manager
// -------------------------------------------------------

class membership_manager {
  public:
    using endpoint_t = asio::ip::tcp::endpoint;
    using on_peer_added_t = std::function<void(server_id, endpoint_t)>;
    using on_peer_removed_t = std::function<void(server_id)>;

    membership_manager(server_id self, asio::io_context& io,
                       asio_transport& transport)
        : self_(self), io_(io), transport_(transport), acceptor_(io) {}

    void set_self_info(const std::string& host, uint16_t raft_port,
                       uint16_t mem_port) {
        self_host_ = host;
        self_raft_port_ = raft_port;
        self_mem_port_ = mem_port;

        member_info mi;
        mi.id = self_;
        mi.host = host;
        mi.raft_port = raft_port;
        mi.mem_port = mem_port;
        members_.push_back(mi);
    }

    void listen(const endpoint_t& mem_ep) {
        acceptor_.open(mem_ep.protocol());
        acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true));
        acceptor_.bind(mem_ep);
        acceptor_.listen();
        do_accept();
    }

    void join(const endpoint_t& bootstrap_mem_ep) {
        logger()->info("server {} joining via {}:{}", self_,
                       bootstrap_mem_ep.address().to_string(),
                       bootstrap_mem_ep.port());

        asio::ip::tcp::socket sock(io_);
        sock.connect(bootstrap_mem_ep);

        mem_message req;
        req.type = mem_msg_type::join_req;
        req.joiner_id = self_;
        req.joiner_host = self_host_;
        req.joiner_raft_port = self_raft_port_;
        req.joiner_mem_port = self_mem_port_;

        msgpack::sbuffer req_buf;
        msgpack::pack(req_buf, req);
        asio::write(sock, asio::buffer(req_buf.data(), req_buf.size()));

        // read join_resp
        msgpack::unpacker unpacker;
        mem_message resp;
        bool got = false;
        while (!got) {
            unpacker.reserve_buffer(4096);
            size_t n = sock.read_some(asio::buffer(unpacker.buffer(), 4096));
            unpacker.buffer_consumed(n);
            msgpack::object_handle oh;
            if (unpacker.next(oh)) {
                oh.get().convert(resp);
                got = true;
            }
        }

        if (resp.members) {
            members_ = *resp.members;
            for (auto& mi : members_) {
                if (mi.id == self_)
                    continue;
                transport_.add_peer(mi.id, mi.endpoint());
                if (on_peer_added_) {
                    on_peer_added_(mi.id, mi.endpoint());
                }
                logger()->info("server {} peer {} added", self_, mi.id);

                asio::ip::tcp::socket asock(io_);
                asock.connect(mi.mem_endpoint());

                mem_message ann;
                ann.type = mem_msg_type::announce;
                ann.joiner_id = self_;
                ann.joiner_host = self_host_;
                ann.joiner_raft_port = self_raft_port_;
                ann.joiner_mem_port = self_mem_port_;

                msgpack::sbuffer ann_buf;
                msgpack::pack(ann_buf, ann);
                asio::write(asock,
                            asio::buffer(ann_buf.data(), ann_buf.size()));
            }
        }
    }

    // Must be called before run() — not thread-safe.
    void set_on_peer_added(on_peer_added_t cb) {
        on_peer_added_ = std::move(cb);
    }

    // Must be called before run() — not thread-safe.
    void set_on_peer_removed(on_peer_removed_t cb) {
        on_peer_removed_ = std::move(cb);
    }

    const std::vector<member_info>& members() const { return members_; }

    void remove_member(server_id id) {
        members_.erase(
            std::remove_if(members_.begin(), members_.end(),
                           [id](const member_info& m) { return m.id == id; }),
            members_.end());
    }

    void notify_leave() {
        mem_message msg;
        msg.type = mem_msg_type::leave_req;
        msg.joiner_id = self_;
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, msg);
        asio::io_context tmp;
        for (auto& mi : members_) {
            if (mi.id == self_)
                continue;
            asio::ip::tcp::socket s(tmp);
            asio::error_code ec;
            s.connect(mi.mem_endpoint(), ec);
            if (!ec)
                asio::write(s, asio::buffer(sbuf.data(), sbuf.size()), ec);
        }
    }

    void broadcast_remove(server_id failed_id) {
        remove_member(failed_id);
        mem_message msg;
        msg.type = mem_msg_type::remove;
        msg.joiner_id = failed_id;
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, msg);
        auto buf = std::make_shared<msgpack::sbuffer>(std::move(sbuf));
        for (auto& mi : members_) {
            if (mi.id == self_)
                continue;
            auto s = std::make_shared<asio::ip::tcp::socket>(io_);
            s->async_connect(
                mi.mem_endpoint(), [s, buf](asio::error_code ec) {
                    if (ec)
                        return;
                    asio::async_write(*s,
                                      asio::buffer(buf->data(), buf->size()),
                                      [s, buf](asio::error_code, size_t) {});
                });
        }
    }

  private:
    void do_accept() {
        auto sock = std::make_shared<asio::ip::tcp::socket>(io_);
        acceptor_.async_accept(*sock, [this, sock](asio::error_code ec) {
            if (!ec) {
                handle_connection(sock);
            }
            do_accept();
        });
    }

    void handle_connection(
        const std::shared_ptr<asio::ip::tcp::socket>& sock) {
        auto unpacker = std::make_shared<msgpack::unpacker>();
        do_mem_read(sock, unpacker);
    }

    void do_mem_read(const std::shared_ptr<asio::ip::tcp::socket>& sock,
                     const std::shared_ptr<msgpack::unpacker>& unpacker) {
        unpacker->reserve_buffer(4096);
        sock->async_read_some(
            asio::buffer(unpacker->buffer(), 4096),
            [this, sock, unpacker](asio::error_code ec, size_t n) {
                if (ec)
                    return;
                unpacker->buffer_consumed(n);
                msgpack::object_handle oh;
                if (unpacker->next(oh)) {
                    mem_message msg;
                    oh.get().convert(msg);
                    handle_mem_message(sock, msg);
                    return;
                }
                do_mem_read(sock, unpacker);
            });
    }

    void handle_mem_message(
        const std::shared_ptr<asio::ip::tcp::socket>& sock,
                            const mem_message& msg) {
        if (msg.type == mem_msg_type::join_req) {
            member_info mi;
            mi.id = msg.joiner_id.value_or(0);
            mi.host = msg.joiner_host.value_or("");
            mi.raft_port = msg.joiner_raft_port.value_or(0);
            mi.mem_port = msg.joiner_mem_port.value_or(0);

            bool dup = false;
            for (auto& x : members_)
                if (x.id == mi.id)
                    dup = true;
            if (!dup)
                members_.push_back(mi);

            logger()->info("server {} accepted join"
                           " from {}",
                           self_, mi.id);

            mem_message resp;
            resp.type = mem_msg_type::join_resp;
            resp.members = members_;

            msgpack::sbuffer resp_sbuf;
            msgpack::pack(resp_sbuf, resp);
            auto buf =
                std::make_shared<msgpack::sbuffer>(std::move(resp_sbuf));
            asio::async_write(*sock, asio::buffer(buf->data(), buf->size()),
                              [sock, buf](asio::error_code, size_t) {});

            transport_.add_peer(mi.id, mi.endpoint());

            if (on_peer_added_) {
                on_peer_added_(mi.id, mi.endpoint());
            }
            logger()->info("server {} peer {} added", self_, mi.id);

        } else if (msg.type == mem_msg_type::announce) {
            member_info mi;
            mi.id = msg.joiner_id.value_or(0);
            mi.host = msg.joiner_host.value_or("");
            mi.raft_port = msg.joiner_raft_port.value_or(0);
            mi.mem_port = msg.joiner_mem_port.value_or(0);

            bool dup = false;
            for (auto& x : members_)
                if (x.id == mi.id)
                    dup = true;
            if (!dup)
                members_.push_back(mi);

            transport_.add_peer(mi.id, mi.endpoint());

            if (on_peer_added_) {
                on_peer_added_(mi.id, mi.endpoint());
            }
            logger()->info("server {} peer {} added", self_, mi.id);
        } else if (msg.type == mem_msg_type::leave_req ||
                   msg.type == mem_msg_type::remove) {
            server_id gone = msg.joiner_id.value_or(0);
            remove_member(gone);
            if (on_peer_removed_)
                on_peer_removed_(gone);
        }
    }

    server_id self_;
    std::string self_host_;
    uint16_t self_raft_port_ = 0;
    uint16_t self_mem_port_ = 0;
    asio::io_context& io_;
    asio_transport& transport_;
    asio::ip::tcp::acceptor acceptor_;
    std::vector<member_info> members_;
    on_peer_added_t on_peer_added_;
    on_peer_removed_t on_peer_removed_;
};

// -------------------------------------------------------
// cluster_node
// -------------------------------------------------------

class cluster_node {
  public:
    using server_t =
        server<asio_transport, file_log_store, log_state_machine>;

    cluster_node(server_id id, uint16_t port,
                 const std::string& advertise_host,
                 const std::string& data_dir = ".")
        : id_(id), raft_port_(port),
          log_store_(make_node_path_(data_dir, id)), transport_(id_, io_),
          mgr_(id_, io_, transport_),
          srv_(id_, {}, transport_, log_store_, sm_), election_timer_(io_),
          heartbeat_timer_(io_), rng_(std::random_device{}()),
          election_dist_(150, 300) {
        log_store_.load();
        auto snap = log_store_.load_snapshot();
        if (snap)
            sm_.install(snap->data);

        using tcp = asio::ip::tcp;
        auto any = asio::ip::address_v4::any();
        transport_.listen(tcp::endpoint(any, raft_port_));
        asio::ip::tcp::resolver resolver(io_);
        auto results =
            resolver.resolve(advertise_host, std::to_string(raft_port_));
        std::string resolved_host =
            results.begin()->endpoint().address().to_string();
        mgr_.set_self_info(resolved_host, raft_port_, raft_port_ + 10000);
        mgr_.listen(tcp::endpoint(any, raft_port_ + 10000));

        transport_.set_callback([this](const message& m) {
            if (m.type != msg_type::client_req)
                srv_.receive(m);
            on_receive_(m);
        });

        mgr_.set_on_peer_added(
            [this](server_id pid, const asio::ip::tcp::endpoint&) {
                asio::post(io_, [this, pid] {
                    srv_.add_peer(pid);
                    last_heard_[pid] = std::chrono ::steady_clock ::now();
                });
            });

        mgr_.set_on_peer_removed([this](server_id pid) {
            asio::post(io_, [this, pid] {
                srv_.remove_peer(pid);
                transport_.remove_peer(pid);
                last_heard_.erase(pid);
            });
        });

        srv_.set_on_peer_removed([this](server_id pid) {
            transport_.remove_peer(pid);
            mgr_.remove_member(pid);
            last_heard_.erase(pid);
        });

        srv_.set_on_peer_added([this](server_id pid) {
            for (auto& mi : mgr_.members()) {
                if (mi.id == pid) {
                    transport_.add_peer(pid, mi.endpoint());
                    last_heard_[pid] = std::chrono ::steady_clock ::now();
                    break;
                }
            }
        });

        srv_.set_on_compact([this] {
            snapshot_t snap;
            snap.index = srv_.snapshot_index();
            snap.term = srv_.snapshot_term();
            snap.data = sm_.snapshot();
            log_store_.save_snapshot(snap);
        });
    }

    void leave() {
        std::thread([this] {
            mgr_.notify_leave();
            io_.stop();
        }).detach();
    }

    // addr must be "host:port".
    // host may be an IP address or hostname.
    void join(const std::string& addr) {
        auto pos = addr.rfind(':');
        std::string host = addr.substr(0, pos);
        uint16_t raft_port =
            static_cast<uint16_t>(std::stoi(addr.substr(pos + 1)));
        uint16_t mem_port = raft_port + 10000;
        asio::ip::tcp::resolver resolver(io_);
        auto results = resolver.resolve(host, std::to_string(mem_port));
        mgr_.join(results.begin()->endpoint());
    }

    void run() {
        asio::signal_set sigs(io_, SIGINT, SIGTERM);
        sigs.async_wait([this](asio::error_code ec, int) {
            if (!ec)
                leave();
        });
        asio::post(io_, [this] { reset_election_timer(); });
        io_.run();
    }

    void stop() { io_.stop(); }

    void submit(std::string data) {
        asio::post(io_, [this, d = std::move(data)] {
            if (is_leader()) {
                srv_.client_request(d);
                for (auto p : srv_.peers())
                    srv_.append_entries(p);
                return;
            }
            if (leader_id_ == nil_id)
                return;
            message m;
            m.type = msg_type::client_req;
            m.term = 0;
            m.from = id_;
            m.to = leader_id_;
            m.client_data = d;
            transport_.send(m);
        });
    }

    // Must be called before run() — not thread-safe.
    void on_apply(std::function<void(const log_entry&)> fn) {
        on_apply_ = std::move(fn);
    }

    server_state state() const { return srv_.state(); }

    bool is_leader() const { return srv_.state() == server_state::leader; }

    bool running() const { return !io_.stopped(); }

    size_t peer_count() const { return srv_.peers().size(); }

  private:
    static std::string make_node_path_(const std::string& data_dir,
                                       server_id id) {
        auto path = data_dir + "/" + std::to_string(id);
        namespace fs = std::filesystem;
        fs::create_directories(path);
        return path + "/raft";
    }

    void reset_election_timer() {
        election_timer_.cancel();
        election_timer_.expires_after(
            std::chrono::milliseconds(election_dist_(rng_)));
        election_timer_.async_wait([this](asio::error_code ec) {
            if (!ec)
                do_election();
        });
    }

    void do_election() {
        srv_.timeout();
        for (auto p : srv_.peers())
            srv_.request_vote(p);
        if (srv_.is_quorum(srv_.votes_granted())) {
            leader_id_ = id_;
            srv_.become_leader();
            start_heartbeat_timer();
            do_heartbeat();
        } else {
            reset_election_timer();
        }
    }

    void on_receive_(const message& m) {
        last_heard_[m.from] = std::chrono::steady_clock::now();
        if (m.type == msg_type::append_entries_req ||
            m.type == msg_type::request_vote_req) {
            reset_election_timer();
        }
        if (m.type == msg_type::append_entries_req) {
            leader_id_ = m.from;
        }
        if (m.type == msg_type::client_req) {
            if (is_leader() && m.client_data) {
                srv_.client_request(*m.client_data);
                for (auto p : srv_.peers())
                    srv_.append_entries(p);
            }
            fire_applied();
            return;
        }
        if (srv_.state() == server_state::candidate &&
            srv_.is_quorum(srv_.votes_granted())) {
            leader_id_ = id_;
            srv_.become_leader();
            start_heartbeat_timer();
            do_heartbeat();
        }
        fire_applied();
    }

    void start_heartbeat_timer() {
        election_timer_.cancel();
        heartbeat_timer_.expires_after(std::chrono::milliseconds(50));
        heartbeat_timer_.async_wait([this](asio::error_code ec) {
            if (ec)
                return;
            if (!is_leader()) {
                reset_election_timer();
                return;
            }
            do_heartbeat();
            start_heartbeat_timer();
        });
    }

    void do_heartbeat() {
        for (auto p : srv_.peers())
            srv_.append_entries(p);
        srv_.advance_commit_index();
        fire_applied();
        if (is_leader()) {
            auto now = std::chrono::steady_clock ::now();
            std::vector<server_id> stale;
            for (auto p : srv_.peers()) {
                auto it = last_heard_.find(p);
                if (it != last_heard_.end() &&
                    now - it->second > removal_timeout_)
                    stale.push_back(p);
            }
            for (auto p : stale) {
                srv_.remove_peer(p);
                transport_.remove_peer(p);
                last_heard_.erase(p);
                mgr_.broadcast_remove(p);
            }
        }
    }

    void fire_applied() {
        const auto& vec = sm_.applied;
        while (applied_up_to_ < vec.size()) {
            const auto& e = vec[applied_up_to_];
            if (on_apply_ && e.type == entry_type::data) {
                on_apply_(e);
            }
            ++applied_up_to_;
        }
    }

    server_id id_;
    uint16_t raft_port_;
    asio::io_context io_;
    file_log_store log_store_;
    log_state_machine sm_;
    asio_transport transport_;
    membership_manager mgr_;
    server_t srv_;
    asio::steady_timer election_timer_;
    asio::steady_timer heartbeat_timer_;
    std::mt19937 rng_;
    std::uniform_int_distribution<int> election_dist_;
    std::function<void(const log_entry&)> on_apply_;
    size_t applied_up_to_ = 0;
    server_id leader_id_ = nil_id;
    std::map<server_id, std::chrono::steady_clock::time_point> last_heard_;
    static constexpr auto removal_timeout_ = std::chrono::seconds(30);
};

} // namespace raftpp

#endif // RAFTPP_H
