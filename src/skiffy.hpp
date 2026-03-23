#ifndef SKIFFY_HPP
#define SKIFFY_HPP

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
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
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "boost/sml.hpp"

#include "msgpack.hpp"

#ifdef SKIFFY_ENABLE_SPDLOG
#include "spdlog/fmt/ostr.h"
#include "spdlog/sinks/null_sink.h"
#include "spdlog/spdlog.h"
#endif

#ifdef SKIFFY_ENABLE_ASIO
#include "asio.hpp"
#endif

namespace skiffy {

using term_t = uint64_t;
using index_t = uint64_t;

enum class entry_type : uint8_t {
    data = 0,
    config_joint = 1,
    config_final = 2,
};

enum class msg_type : uint8_t {
    request_vote_req = 0,
    request_vote_resp = 1,
    append_entries_req = 2,
    append_entries_resp = 3,
    install_snapshot_req = 4,
    install_snapshot_resp = 5,
    client_fwd = 6,
    mem_join_req = 7,
    mem_join_resp = 8,
    mem_announce = 9,
    mem_remove = 10,
};

// -------------------------------------------------------
// node_id
// -------------------------------------------------------
class node_id {
  public:
    const std::array<uint8_t, 16> addr_ = {};
    const uint16_t port_ = 0;

    constexpr node_id() = default;

    constexpr node_id(const std::array<uint8_t, 16>& addr, uint16_t port)
        : addr_(addr), port_(port), id_(make_id_(addr, port)) {}

    template <size_t N, std::enable_if_t<N == 4, int> = 0>
    constexpr node_id(const std::array<uint8_t, N>& a, uint16_t p)
        : node_id(make_addr_(a), p) {}

    template <size_t N, std::enable_if_t<N == 4 || N == 16, int> = 0>
    constexpr node_id(const uint8_t (&a)[N], uint16_t p)
        : node_id(a, p, std::make_index_sequence<N>{}) {}

    node_id(const node_id&) = default;
    node_id(node_id&&) = default;

    node_id& operator=(const node_id& o) noexcept {
        if (this != &o)
            return *::new (this) node_id(o);
        return *this;
    }

    node_id& operator=(node_id&& o) noexcept {
        if (this != &o)
            return *::new (this) node_id(std::move(o));
        return *this;
    }

    constexpr operator std::string_view() const {
        return {id_.data(), is_v4() ? 13ul : 36ul};
    }

    constexpr bool is_nil() const {
        return port_ == 0 && addr_ == std::array<uint8_t, 16>{};
    }

    constexpr bool is_v4() const {
        return addr_[10] == 0xff && addr_[11] == 0xff && addr_[0] == 0 &&
            addr_[1] == 0;
    }

    bool operator==(const node_id& o) const {
        return addr_ == o.addr_ && port_ == o.port_;
    }

    bool operator!=(const node_id& o) const { return !(*this == o); }

    bool operator<(const node_id& o) const {
        if (addr_ != o.addr_)
            return addr_ < o.addr_;
        return port_ < o.port_;
    }

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        pk.pack_bin(18);
        pk.pack_bin_body(reinterpret_cast<const char*>(addr_.data()), 16);
        uint8_t pb[2] = {uint8_t(port_ >> 8), uint8_t(port_)};
        pk.pack_bin_body(reinterpret_cast<const char*>(pb), 2);
    }

    void msgpack_unpack(msgpack::object const& o) {
        if (o.type != msgpack::type::BIN || o.via.bin.size < 18)
            return;
        const auto* p = reinterpret_cast<const uint8_t*>(o.via.bin.ptr);
        std::array<uint8_t, 16> a;
        std::copy(p, p + 16, a.begin());
        uint16_t port = static_cast<uint16_t>((uint16_t{p[16]} << 8) | p[17]);
        ::new (this) node_id(a, port);
    }

  private:
    template <size_t N, size_t... I>
    constexpr node_id(const uint8_t (&a)[N], uint16_t p,
                      std::index_sequence<I...>)
        : node_id(std::array<uint8_t, N>({a[I]...}), p) {}

    template <size_t N>
    static constexpr std::array<uint8_t, 16>
    make_addr_(const std::array<uint8_t, N>& a) {
        return {0, 0, 0,    0,    0,    0,    0,    0,
                0, 0, 0xff, 0xff, a[0], a[1], a[2], a[3]};
    }

    static constexpr std::array<char, 37>
    make_id_(const std::array<uint8_t, 16>& a, uint16_t port) {
        constexpr char hex[] = "0123456789abcdef";
        bool is_v4 = a[10] == 0xff && a[11] == 0xff && a[0] == 0 && a[1] == 0;
        uint8_t p1 = static_cast<uint8_t>(port >> 8);
        uint8_t p2 = static_cast<uint8_t>(port & 0xff);
        if (is_v4) {
            return {
                // clang-format off
                hex[(a[12] >> 4)], hex[(a[12] & 0xf)],
                hex[(a[13] >> 4)], hex[(a[13] & 0xf)],
                hex[(a[14] >> 4)], hex[(a[14] & 0xf)],
                hex[(a[15] >> 4)], hex[(a[15] & 0xf)], '-',
                hex[(p1    >> 4)], hex[(p1    & 0xf)],
                hex[(p2    >> 4)], hex[(p2    & 0xf)], '\0'
                // clang-format on
            };
        }
        return {
            // clang-format off
            hex[(a[ 0] >> 4)], hex[(a[ 0] & 0xf)],
            hex[(a[ 1] >> 4)], hex[(a[ 1] & 0xf)],
            hex[(a[ 2] >> 4)], hex[(a[ 2] & 0xf)],
            hex[(a[ 3] >> 4)], hex[(a[ 3] & 0xf)], '-',
            hex[(a[ 4] >> 4)], hex[(a[ 4] & 0xf)],
            hex[(a[ 5] >> 4)], hex[(a[ 5] & 0xf)], '-',
            hex[((a[6] ^ p1) >> 4)], hex[((a[6] ^ p1) & 0xf)],
            hex[((a[7] ^ p2) >> 4)], hex[((a[7] ^ p2) & 0xf)], '-',
            hex[(a[ 8] >> 4)], hex[(a[ 8] & 0xf)],
            hex[(a[ 9] >> 4)], hex[(a[ 9] & 0xf)], '-',
            hex[(a[10] >> 4)], hex[(a[10] & 0xf)],
            hex[(a[11] >> 4)], hex[(a[11] & 0xf)],
            hex[(a[12] >> 4)], hex[(a[12] & 0xf)],
            hex[(a[13] >> 4)], hex[(a[13] & 0xf)],
            hex[(a[14] >> 4)], hex[(a[14] & 0xf)],
            hex[(a[15] >> 4)], hex[(a[15] & 0xf)], '\0'
            // clang-format on
        };
    }

    std::array<char, 37> id_ = {};
};

constexpr node_id nil_id{};

inline std::string_view format_as(const node_id& sid) {
    return static_cast<std::string_view>(sid);
}

// -------------------------------------------------------
// core data structures
// -------------------------------------------------------
struct log_entry {
    static constexpr size_t field_count = 3;

    term_t term = 0;
    entry_type type = entry_type::data;
    std::string value;

    bool operator==(const log_entry& o) const {
        return term == o.term && type == o.type && value == o.value;
    }
    bool operator!=(const log_entry& o) const { return !(*this == o); }

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        pk.pack_array(field_count);
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

struct snapshot_entry {
    static constexpr size_t field_count = 3;

    index_t index = 0;
    term_t term = 0;
    std::string data;

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        pk.pack_array(field_count);
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
    static constexpr size_t field_count = 17;

    msg_type type;
    term_t term = 0;
    node_id from;
    node_id to;

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

    // client_fwd field
    std::optional<std::string> payload;

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
            snapshot_data == o.snapshot_data && payload == o.payload;
    }
    bool operator!=(const message& o) const { return !(*this == o); }

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        pk.pack_array(field_count);
        pk.pack(static_cast<uint8_t>(type));
        pk.pack(term);
        pk.pack(from);
        pk.pack(to);
        pk.pack(last_log_term);
        pk.pack(last_log_index);
        pk.pack(vote_granted);
        pk.pack(prev_log_index);
        pk.pack(prev_log_term);
        pk.pack(entries);
        pk.pack(commit_index);
        pk.pack(success);
        pk.pack(match_index);
        pk.pack(snapshot_index);
        pk.pack(snapshot_term);
        pk.pack(snapshot_data);
        pk.pack(payload);
    }
    void msgpack_unpack(msgpack::object const& o) {
        if (o.type != msgpack::type::ARRAY || o.via.array.size < field_count)
            throw msgpack::type_error{};
        auto& a = o.via.array;
        type = static_cast<msg_type>(a.ptr[0].as<uint8_t>());
        term = a.ptr[1].as<term_t>();
        from = a.ptr[2].as<node_id>();
        to = a.ptr[3].as<node_id>();
        a.ptr[4].convert(last_log_term);
        a.ptr[5].convert(last_log_index);
        a.ptr[6].convert(vote_granted);
        a.ptr[7].convert(prev_log_index);
        a.ptr[8].convert(prev_log_term);
        a.ptr[9].convert(entries);
        a.ptr[10].convert(commit_index);
        a.ptr[11].convert(success);
        a.ptr[12].convert(match_index);
        a.ptr[13].convert(snapshot_index);
        a.ptr[14].convert(snapshot_term);
        a.ptr[15].convert(snapshot_data);
        a.ptr[16].convert(payload);
    }
};

// -------------------------------------------------------
// LogStore concept
//
// LogStore must provide:
//   void append(const log_entry&)
//   void truncate(size_t n)
//   void clear()
//   void reserve(size_t n)         -- capacity hint
//   size_t size() const
//   bool empty() const
//   const log_entry& operator[](size_t) const
//   log_entry& operator[](size_t)
// -------------------------------------------------------
struct memory_log_store {
    void append(const log_entry& e) { entries_.push_back(e); }
    void truncate(size_t n) { entries_.resize(n); }

    void clear() { entries_.clear(); }
    void reserve(size_t n) { entries_.reserve(n); }
    size_t size() const { return entries_.size(); }
    bool empty() const { return entries_.empty(); }

    const log_entry& operator[](size_t i) const { return entries_[i]; }
    log_entry& operator[](size_t i) { return entries_[i]; }

    // no-op persistence interface (mirrors file_log_store)
    void load() {}
    void save_snapshot(const snapshot_entry&) {}
    std::optional<snapshot_entry> load_snapshot() { return std::nullopt; }

  private:
    std::vector<log_entry> entries_;
};

namespace detail {

// -------------------------------------------------------
// logger -- either spdlog or constexpr no-ops
// -------------------------------------------------------
#ifdef SKIFFY_ENABLE_SPDLOG
inline spdlog::logger& logger() {
    static auto sp = []() {
        auto l = spdlog::get("skiffy");
        if (!l) {
            l = std::make_shared<spdlog::logger>(
                "skiffy", std::make_shared<spdlog::sinks::null_sink_mt>());
            spdlog::register_logger(l);
        }
        return l;
    }();
    return *sp;
}
#else
struct logger_t {
    // clang-format off
    template <typename... A> constexpr void debug(A&&...) const noexcept {}
    template <typename... A> constexpr void info(A&&...) const noexcept {}
    template <typename... A> constexpr void warn(A&&...) const noexcept {}
    template <typename... A> constexpr void error(A&&...) const noexcept {}
    // clang-format on
};
inline const auto& logger() {
    static logger_t logger;
    return logger;
}
#endif

enum class server_state { follower, candidate, leader };

// clang-format off
struct evt_timeout {};
struct evt_restart {};
struct evt_become_leader {};
struct evt_advance {};
struct evt_higher_term { term_t term; };

// inbound message events
struct evt_vote_req { const message& m; };
struct evt_vote_resp { const message& m; };
struct evt_append_req { const message& m; };
struct evt_append_resp { const message& m; };
struct evt_snapshot_req { const message& m; };
struct evt_snapshot_resp { const message& m; };
struct evt_client_req { std::string v; };

// outbound action events
struct evt_vote_request_to { node_id j; };
struct evt_append_entries_to { node_id j; };
struct evt_config_req { std::set<node_id> peers; };
// clang-format on

// -------------------------------------------------------
// server
// -------------------------------------------------------
template <typename Transport, typename LogStore = memory_log_store>
class server {
    // sml state tags
    struct s_follower {};
    struct s_candidate {};
    struct s_leader {};

    // --- SML state machine definition ---
    struct raft_state_machine {
        auto operator()() const noexcept {
            namespace sml = boost::sml;

            auto fs = sml::state<s_follower>;
            auto cs = sml::state<s_candidate>;
            auto ls = sml::state<s_leader>;

            // guards
            auto has_quorum = [](server& s) noexcept {
                return s.is_quorum(s.votes_granted_);
            };
            auto same_term = [](const evt_append_req& e, server& s) noexcept {
                return e.m.term == s.current_term_;
            };
            auto not_yet_asked = [](const evt_vote_request_to& e,
                                    server& s) noexcept {
                return !s.votes_responded_.count(e.j);
            };

            // actions
            auto do_start_election = [](server& s) noexcept {
                s.do_timeout_action();
            };
            auto do_restart = [](server& s) noexcept {
                s.do_restart_action();
            };
            auto do_update_term = [](const evt_higher_term& e,
                                     server& s) noexcept {
                s.do_update_term_action(e.term);
            };
            auto do_init_leader = [](server& s) noexcept {
                s.do_become_leader_action();
            };
            auto do_send_rv = [](const evt_vote_request_to& e,
                                 server& s) noexcept {
                s.do_send_rv_req_action(e.j);
            };
            auto do_rv_req = [](const evt_vote_req& e, server& s) noexcept {
                s.handle_request_vote_request(e.m);
            };
            auto do_rv_resp = [](const evt_vote_resp& e, server& s) noexcept {
                s.handle_request_vote_response(e.m);
            };
            auto do_ae_req = [](const evt_append_req& e, server& s) noexcept {
                s.handle_append_entries_request(e.m);
            };
            auto do_ae_resp = [](const evt_append_resp& e,
                                 server& s) noexcept {
                s.handle_append_entries_response(e.m);
            };
            auto do_snap_req = [](const evt_snapshot_req& e,
                                  server& s) noexcept {
                s.handle_install_snapshot_request(e.m);
            };
            auto do_snap_resp = [](const evt_snapshot_resp& e,
                                   server& s) noexcept {
                s.handle_install_snapshot_response(e.m);
            };
            auto do_client = [](const evt_client_req& e, server& s) noexcept {
                s.do_client_request_action(e.v);
            };
            auto do_config = [](const evt_config_req& e, server& s) noexcept {
                s.do_config_request_action(e.peers);
            };
            auto do_ae_to = [](const evt_append_entries_to& e,
                               server& s) noexcept {
                s.do_append_entries_action(e.j);
            };
            auto do_advance = [](server& s) noexcept {
                s.do_advance_commit_action();
            };

            // transition table
            return sml::make_transition_table(
                // follower
                *fs + sml::event<evt_timeout> / do_start_election = cs,

                fs + sml::event<evt_restart> / do_restart = fs,

                fs + sml::event<evt_higher_term> / do_update_term = fs,

                fs + sml::event<evt_vote_req> / do_rv_req = fs,

                fs + sml::event<evt_append_req> / do_ae_req = fs,

                fs + sml::event<evt_snapshot_req> / do_snap_req = fs,

                // candidate
                cs + sml::event<evt_timeout> / do_start_election = cs,

                cs + sml::event<evt_restart> / do_restart = fs,

                cs + sml::event<evt_higher_term> / do_update_term = fs,

                cs + sml::event<evt_become_leader>[has_quorum] /
                        do_init_leader = ls,

                cs + sml::event<evt_vote_request_to>[not_yet_asked] /
                        do_send_rv = cs,

                cs + sml::event<evt_vote_req> / do_rv_req = cs,

                cs + sml::event<evt_vote_resp> / do_rv_resp = cs,

                // candidate receiving same-term AppendEntries
                // steps down without processing the message
                cs + sml::event<evt_append_req>[same_term] = fs,

                cs + sml::event<evt_snapshot_req> / do_snap_req = cs,

                // leader
                ls + sml::event<evt_restart> / do_restart = fs,

                ls + sml::event<evt_higher_term> / do_update_term = fs,

                ls + sml::event<evt_vote_req> / do_rv_req = ls,

                ls + sml::event<evt_append_resp> / do_ae_resp = ls,

                ls + sml::event<evt_snapshot_resp> / do_snap_resp = ls,

                ls + sml::event<evt_append_req> / do_ae_req = ls,

                ls + sml::event<evt_client_req> / do_client = ls,

                ls + sml::event<evt_config_req> / do_config = ls,

                ls + sml::event<evt_append_entries_to> / do_ae_to = ls,

                ls + sml::event<evt_advance> / do_advance = ls);
        }
    };

  public:
    server(node_id self, std::set<node_id> peers, Transport& transport,
           LogStore& ls)
        : log_store_(ls), transport_(transport), id_(self),
          peers_(std::move(peers)), sm_{*this} {
        init_state();
    }

    server(const server&) = delete;
    server& operator=(const server&) = delete;
    server(server&&) = delete;
    server& operator=(server&&) = delete;

    void restart() { sm_.process_event(evt_restart{}); }
    void timeout() { sm_.process_event(evt_timeout{}); }
    void request_vote(node_id j) {
        sm_.process_event(evt_vote_request_to{j});
    }
    void become_leader() { sm_.process_event(evt_become_leader{}); }
    std::optional<index_t> client_request(std::string v) {
        auto before = last_log_index();
        sm_.process_event(evt_client_req{std::move(v)});
        auto after = last_log_index();
        if (after > before)
            return after;
        return std::nullopt;
    }
    void config_request(std::set<node_id> new_peers) {
        sm_.process_event(evt_config_req{std::move(new_peers)});
    }
    void append_entries(node_id j) {
        sm_.process_event(evt_append_entries_to{j});
    }
    void advance_commit_index() { sm_.process_event(evt_advance{}); }

    void compact_threshold(size_t n) {
        compact_threshold_ = n;
        log_store_.reserve(n);
    }

    void on_peer_removed(std::function<void(node_id)> cb) {
        on_peer_removed_ = std::move(cb);
    }

    void on_peer_added(std::function<void(node_id)> cb) {
        on_peer_added_ = std::move(cb);
    }

    void on_compact(std::function<void()> cb) { on_compact_ = std::move(cb); }

    void on_become_leader(std::function<void()> cb) {
        on_become_leader_ = std::move(cb);
    }

    void on_entries_dropped(std::function<void(std::vector<log_entry>)> cb) {
        on_entries_dropped_ = std::move(cb);
    }

    void compact() {
        if (commit_index_ <= snapshot_index_)
            return;
        auto new_snap = commit_index_;
        auto new_term = entry_term(new_snap);
        auto keep_from = new_snap - log_base_;
        std::vector<log_entry> tail;
        for (size_t i = keep_from; i < log_store_.size(); ++i)
            tail.push_back(log_store_[i]);
        log_store_.clear();
        log_base_ = new_snap;
        for (auto& e : tail)
            log_store_.append(e);
        snapshot_index_ = new_snap;
        snapshot_term_ = new_term;
        if (on_compact_)
            on_compact_();
    }

    void receive(const message& m) {
        if (m.to != id_) {
            logger().warn("server {} recv msg for {}, dropping", id_, m.to);
            return;
        }
        try {
            logger().debug("server {} recv mtype={} from {}", id_,
                           static_cast<int>(m.type), m.from);

            if (m.term > current_term_) {
                sm_.process_event(evt_higher_term{m.term});
            }

            switch (m.type) {
                case msg_type::request_vote_req:
                    sm_.process_event(evt_vote_req{m});
                    break;
                case msg_type::request_vote_resp:
                    if (!drop_stale_response(m)) {
                        sm_.process_event(evt_vote_resp{m});
                    } else {
                        logger().warn(
                            "server {} stale request_vote_resp from {}", id_,
                            m.from);
                    }
                    break;
                case msg_type::append_entries_req:
                    sm_.process_event(evt_append_req{m});
                    break;
                case msg_type::append_entries_resp:
                    if (!drop_stale_response(m)) {
                        sm_.process_event(evt_append_resp{m});
                    } else {
                        logger().warn(
                            "server {} stale append_entries_resp from {}",
                            id_, m.from);
                    }
                    break;
                case msg_type::install_snapshot_req:
                    sm_.process_event(evt_snapshot_req{m});
                    break;
                case msg_type::install_snapshot_resp:
                    if (!drop_stale_response(m)) {
                        sm_.process_event(evt_snapshot_resp{m});
                    }
                    break;
                case msg_type::client_fwd:
                    if (is_leader() && m.payload)
                        client_request(*m.payload);
                    break;
                default:
                    break;
            }
        } catch (const std::exception& e) {
            logger().warn("server {} dropped message: {}", id_, e.what());
        }
    }

    void add_peer(node_id j) {
        peers_.insert(j);
        next_index_[j] = last_log_index() + 1;
        match_index_[j] = 0;
    }

    void remove_peer(node_id j) {
        peers_.erase(j);
        next_index_.erase(j);
        match_index_.erase(j);
    }

    node_id id() const { return id_; }
    server_state state() const {
        if (sm_.is(boost::sml::state<s_leader>))
            return server_state::leader;
        if (sm_.is(boost::sml::state<s_candidate>))
            return server_state::candidate;
        return server_state::follower;
    }
    index_t snapshot_index() const { return snapshot_index_; }
    term_t snapshot_term() const { return snapshot_term_; }
    const std::set<node_id>& votes_granted() const { return votes_granted_; }
    const std::set<node_id>& peers() const { return peers_; }
    const std::vector<log_entry>& applied() const { return applied_; }

    void load_snapshot(const snapshot_entry& snap) {
        auto oh = msgpack::unpack(snap.data.data(), snap.data.size());
        oh.get().convert(applied_);
        snapshot_index_ = snap.index;
        snapshot_term_ = snap.term;
        last_applied_ = snap.index;
        commit_index_ = snap.index;
        log_base_ = snap.index;
    }

    void prune_applied(size_t n) {
        if (n >= applied_.size()) {
            applied_.clear();
            return;
        }
        applied_.erase(applied_.begin(), applied_.begin() + n);
    }

    bool is_quorum(const std::set<node_id>& s) const {
        if (!joint_config_) {
            return s.size() * 2 > peers_.size() + 1;
        }
        // joint consensus: majority in both configs
        return is_quorum_cnt_([&](const std::set<node_id>& cfg) {
            size_t c = 0;
            for (auto sid : s)
                if (sid == id_ || cfg.count(sid))
                    ++c;
            return c;
        });
    }

  private:
    void init_state() {
        current_term_ = 1;
        voted_for_ = nil_id;
        commit_index_ = 0;
        last_applied_ = 0;
        snapshot_index_ = 0;
        snapshot_term_ = 0;
        log_base_ = 0;
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

    term_t last_term() const {
        if (log_store_.empty())
            return snapshot_term_;
        return log_store_[log_store_.size() - 1].term;
    }

    index_t last_log_index() const { return log_base_ + log_store_.size(); }

    size_t log_offset(index_t idx) const { return idx - log_base_ - 1; }

    term_t entry_term(index_t idx) const {
        return log_store_[log_offset(idx)].term;
    }

    void do_update_term_action(term_t new_term) {
        logger().debug("server {} term {} -> {}", id_, current_term_,
                       new_term);
        current_term_ = new_term;
        voted_for_ = nil_id;
    }

    bool drop_stale_response(const message& m) const {
        return m.term < current_term_;
    }

    void handle_request_vote_request(const message& m) {
        auto j = m.from;
        auto m_last_log_term = m.last_log_term.value_or(0);
        auto m_last_log_index = m.last_log_index.value_or(0);

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

        auto j = m.from;
        votes_responded_.insert(j);

        if (m.vote_granted.value_or(false)) {
            votes_granted_.insert(j);
        }
    }

    void handle_append_entries_request(const message& m) {
        auto j = m.from;
        auto prev_idx = m.prev_log_index.value_or(0);
        auto prev_term = m.prev_log_term.value_or(0);
        static const std::vector<log_entry> empty_entries;
        const auto& entries = m.entries ? *m.entries : empty_entries;
        auto m_commit = m.commit_index.value_or(0);

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
            auto idx = prev_idx + 1 + static_cast<index_t>(i);
            if (idx <= snapshot_index_)
                continue;
            auto si = log_offset(idx);
            if (si < log_store_.size()) {
                if (log_store_[si].term != entries[i].term) {
                    revert_config_if_needed(si);
                    if (on_entries_dropped_) {
                        std::vector<log_entry> dropped;
                        for (size_t k = si; k < log_store_.size(); ++k)
                            dropped.push_back(log_store_[k]);
                        on_entries_dropped_(std::move(dropped));
                    }
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

        auto j = m.from;
        if (!peers_.count(j))
            return;
        if (m.success.value_or(false)) {
            auto mi = m.match_index.value_or(0);
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
        auto si = m.snapshot_index.value_or(0);
        auto st = m.snapshot_term.value_or(0);
        if (snapshot_index_ >= si)
            return;

        {
            const auto& d = m.snapshot_data.value_or("");
            auto oh = msgpack::unpack(d.data(), d.size());
            oh.get().convert(applied_);
        }
        snapshot_index_ = si;
        snapshot_term_ = st;
        last_applied_ = snapshot_index_;
        commit_index_ = snapshot_index_;
        log_store_.clear();
        log_base_ = si;

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
        auto j = m.from;
        auto si = m.snapshot_index.value_or(0);
        next_index_[j] = si + 1;
        match_index_[j] = si;
    }

    bool log_has_config_final() const {
        for (size_t i = 0; i < log_store_.size(); ++i)
            if (log_store_[i].type == entry_type::config_final)
                return true;
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
        std::set<node_id> np;
        auto oh = msgpack::unpack(e.value.data(), e.value.size());
        oh.get().convert(np);
        joint_config_ = np;
        if (is_leader())
            ensure_config_final();
    }

    void apply_config_final(const log_entry& e) {
        std::set<node_id> np;
        auto oh = msgpack::unpack(e.value.data(), e.value.size());
        oh.get().convert(np);
        for (auto p : np) {
            if (p != id_ && !peers_.count(p)) {
                add_peer(p);
                if (on_peer_added_)
                    on_peer_added_(p);
            }
        }
        for (auto p : peers_)
            if (!np.count(p)) {
                next_index_.erase(p);
                match_index_.erase(p);
                if (on_peer_removed_)
                    on_peer_removed_(p);
            }
        peers_ = np;
        peers_.erase(id_);
        committed_peers_ = peers_;
        joint_config_ = std::nullopt;
    }

    void revert_config_if_needed(size_t from_offset) {
        for (size_t i = from_offset; i < log_store_.size(); ++i) {
            auto idx = snapshot_index_ + 1 + static_cast<index_t>(i);
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
                applied_.push_back(e);
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

    template <typename F>
    bool is_quorum_cnt_(F count_in) const {
        if (!joint_config_)
            return count_in(peers_) * 2 > peers_.size() + 1;
        return count_in(peers_) * 2 > peers_.size() + 1 &&
            count_in(*joint_config_) * 2 > joint_config_->size() + 1;
    }

    // --- SML action methods ---
    void do_timeout_action() {
        current_term_++;
        voted_for_ = id_;
        votes_responded_.clear();
        votes_granted_.clear();
        votes_granted_.insert(id_);
        logger().info("server {} timeout, starting election term {}", id_,
                      current_term_);
    }

    void do_restart_action() {
        votes_responded_.clear();
        votes_granted_.clear();
        init_leader_vars();
        commit_index_ = 0;
        // currentTerm, votedFor, log preserved
    }

    void do_become_leader_action() {
        auto next = last_log_index() + 1;
        for (auto& p : peers_) {
            next_index_[p] = next;
            match_index_[p] = 0;
        }
        next_index_[id_] = next;
        match_index_[id_] = 0;
        if (joint_config_.has_value())
            ensure_config_final();
        logger().info("server {} became leader term {}", id_, current_term_);
        if (on_become_leader_)
            on_become_leader_();
    }

    void do_send_rv_req_action(node_id j) {
        logger().debug("server {} send request_vote to {}", id_, j);
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

    void do_config_request_action(const std::set<node_id>& new_peers) {
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

    void do_append_entries_action(node_id j) {
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
            {
                msgpack::sbuffer buf;
                msgpack::pack(buf, applied_);
                m.snapshot_data =
                    std::string(buf.data(), buf.data() + buf.size());
            }
            transport_.send(m);
            return;
        }

        auto prev_log_index = next_index_[j] - 1;
        term_t prev_log_term = 0;
        if (prev_log_index > snapshot_index_) {
            prev_log_term = entry_term(prev_log_index);
        } else if (prev_log_index == snapshot_index_ && snapshot_index_ > 0) {
            prev_log_term = snapshot_term_;
        }

        auto hi = last_log_index();
        std::vector<log_entry> entries;
        if (next_index_[j] <= hi)
            entries.reserve(hi - next_index_[j] + 1);
        for (index_t i = next_index_[j]; i <= hi; ++i) {
            entries.push_back(log_store_[log_offset(i)]);
        }

        auto last_entry =
            prev_log_index + static_cast<index_t>(entries.size());
        auto commit = std::min(commit_index_, last_entry);

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
        index_t max_agree = 0;
        for (index_t idx = last_log_index(); idx > snapshot_index_; --idx) {
            auto count_in = [&](const std::set<node_id>& cfg) {
                size_t c = 1; // self always agrees
                for (auto& p : cfg) {
                    auto it = match_index_.find(p);
                    if (it != match_index_.end() && it->second >= idx)
                        ++c;
                }
                return c;
            };
            if (is_quorum_cnt_(count_in)) {
                max_agree = idx;
                break;
            }
        }
        if (max_agree > 0 && entry_term(max_agree) == current_term_)
            commit_index_ = max_agree;
        apply_committed();
    }

    LogStore& log_store_;
    std::vector<log_entry> applied_;
    Transport& transport_;

    // --- identity ---
    node_id id_;
    std::set<node_id> peers_;

    // serverVars
    term_t current_term_ = 1;
    node_id voted_for_ = nil_id;

    // logVars
    index_t commit_index_ = 0;

    // snapshot state
    index_t snapshot_index_ = 0;
    term_t snapshot_term_ = 0;
    index_t last_applied_ = 0;
    index_t log_base_ = 0;
    size_t compact_threshold_ = 0;

    // membership
    std::optional<std::set<node_id>> joint_config_;
    std::set<node_id> committed_peers_;
    std::function<void(node_id)> on_peer_removed_;
    std::function<void(node_id)> on_peer_added_;
    std::function<void()> on_compact_;
    std::function<void()> on_become_leader_;
    std::function<void(std::vector<log_entry>)> on_entries_dropped_;

    // candidateVars
    std::set<node_id> votes_responded_;
    std::set<node_id> votes_granted_;

    // leaderVars
    std::map<node_id, index_t> next_index_;
    std::map<node_id, index_t> match_index_;

    // sm_ must be last: members must be initialized before SM starts
    mutable boost::sml::sm<raft_state_machine> sm_;

    // friend for internal state access for testing
    template <typename, typename>
    friend struct test_server;
};

inline constexpr uint32_t crc32(const char* data, size_t n) {
    uint32_t crc = 0xffffffff;
    for (size_t i = 0; i < n; ++i) {
        crc ^= static_cast<uint8_t>(data[i]);
        for (int k = 0; k < 8; ++k)
            crc = (crc >> 1) ^ (crc & 1 ? 0xedb88320u : 0u);
    }
    return crc ^ 0xffffffff;
}

// -------------------------------------------------------
// membership_manager
// -------------------------------------------------------
class membership_manager {
  public:
    explicit membership_manager(node_id self) : self_(self) {
        members_.push_back(self);
    }

    message join(node_id bootstrap) {
        msgpack::sbuffer buf;
        msgpack::pack(buf, self_);
        message req;
        req.type = msg_type::mem_join_req;
        req.from = self_;
        req.to = bootstrap;
        req.payload = std::string(buf.data(), buf.data() + buf.size());
        return req;
    }

    std::vector<message> receive(const message& m) {
        switch (m.type) {
            case msg_type::mem_join_req:
                return handle_join_req(m);
            case msg_type::mem_join_resp:
                return handle_join_resp(m);
            case msg_type::mem_announce:
                handle_announce(m);
                return {};
            case msg_type::mem_remove:
                handle_remove(m);
                return {};
            default:
                return {};
        }
    }

    std::vector<message> notify_leave() {
        msgpack::sbuffer buf;
        msgpack::pack(buf, self_);
        std::string payload(buf.data(), buf.data() + buf.size());
        std::vector<message> out;
        for (auto& mi : members_) {
            if (mi == self_)
                continue;
            message msg;
            msg.type = msg_type::mem_remove;
            msg.from = self_;
            msg.to = mi;
            msg.payload = payload;
            out.push_back(std::move(msg));
        }
        return out;
    }

    std::vector<message> broadcast_remove(node_id failed_id) {
        remove_member(failed_id);
        msgpack::sbuffer buf;
        msgpack::pack(buf, failed_id);
        std::string payload(buf.data(), buf.data() + buf.size());
        std::vector<message> out;
        for (auto& mi : members_) {
            if (mi == self_)
                continue;
            message msg;
            msg.type = msg_type::mem_remove;
            msg.from = self_;
            msg.to = mi;
            msg.payload = payload;
            out.push_back(std::move(msg));
        }
        return out;
    }

    void remove_member(node_id id) {
        members_.erase(
            std::remove_if(members_.begin(), members_.end(),
                           [id](const node_id& m) { return m == id; }),
            members_.end());
    }

    void on_peer_added(std::function<void(node_id)> cb) {
        on_peer_added_ = std::move(cb);
    }

    void on_peer_removed(std::function<void(node_id)> cb) {
        on_peer_removed_ = std::move(cb);
    }

    const std::vector<node_id>& members() const { return members_; }

  private:
    std::vector<message> do_add_member(node_id id) {
        for (auto& x : members_)
            if (x == id)
                return {};
        members_.push_back(id);
        if (on_peer_added_)
            on_peer_added_(id);
        return {};
    }

    std::vector<message> handle_join_req(const message& m) {
        if (!m.payload)
            return {};
        auto oh = msgpack::unpack(m.payload->data(), m.payload->size());
        node_id joiner;
        oh.get().convert(joiner);
        do_add_member(joiner);
        msgpack::sbuffer buf;
        msgpack::pack(buf, members_);
        message resp;
        resp.type = msg_type::mem_join_resp;
        resp.from = self_;
        resp.to = joiner;
        resp.payload = std::string(buf.data(), buf.data() + buf.size());
        return {resp};
    }

    std::vector<message> handle_join_resp(const message& m) {
        if (!m.payload)
            return {};
        auto oh = msgpack::unpack(m.payload->data(), m.payload->size());
        std::vector<node_id> mems;
        oh.get().convert(mems);
        members_ = mems;
        msgpack::sbuffer buf;
        msgpack::pack(buf, self_);
        std::string payload(buf.data(), buf.data() + buf.size());
        std::vector<message> out;
        for (auto& mi : members_) {
            if (mi == self_)
                continue;
            if (on_peer_added_)
                on_peer_added_(mi);
            message ann;
            ann.type = msg_type::mem_announce;
            ann.from = self_;
            ann.to = mi;
            ann.payload = payload;
            out.push_back(std::move(ann));
        }
        return out;
    }

    void handle_announce(const message& m) {
        if (!m.payload)
            return;
        auto oh = msgpack::unpack(m.payload->data(), m.payload->size());
        node_id joiner;
        oh.get().convert(joiner);
        do_add_member(joiner);
    }

    void handle_remove(const message& m) {
        if (!m.payload)
            return;
        auto oh = msgpack::unpack(m.payload->data(), m.payload->size());
        node_id gone;
        oh.get().convert(gone);
        remove_member(gone);
        if (on_peer_removed_)
            on_peer_removed_(gone);
    }

    node_id self_;
    std::vector<node_id> members_;
    std::function<void(node_id)> on_peer_added_;
    std::function<void(node_id)> on_peer_removed_;
};

} // namespace detail

// -------------------------------------------------------
// file_log_store
// -------------------------------------------------------

struct file_log_store {
    file_log_store() = default;
    explicit file_log_store(const std::string& path_prefix)
        : wal_path_(path_prefix + ".wal"), snap_path_(path_prefix + ".snap") {
        auto parent = std::filesystem::path(path_prefix).parent_path();
        if (!parent.empty())
            std::filesystem::create_directories(parent);
        load();
    }

    void load() {
        std::ifstream f(wal_path_, std::ios::binary);
        if (!f) {
            open_wal_append();
            return;
        }
        std::string data(std::istreambuf_iterator<char>(f), {});
        if (f.bad())
            detail::logger().warn("file_log_store: read error on {}",
                                  wal_path_);
        size_t pos = 0;
        while (pos < data.size()) {
            if (data.size() - pos < 8) {
                detail::logger().warn("file_log_store: truncated wal,"
                                      " recovering");
                rewrite_wal();
                return;
            }
            auto sz = read_le32(data.data() + pos);
            auto stored = read_le32(data.data() + pos + 4);
            pos += 8;
            if (data.size() - pos < sz) {
                detail::logger().warn("file_log_store: truncated wal,"
                                      " recovering");
                rewrite_wal();
                return;
            }
            auto actual = detail::crc32(data.data() + pos, sz);
            if (actual != stored)
                throw std::runtime_error("file_log_store: corrupt wal");
            auto oh = msgpack::unpack(data.data() + pos, sz);
            log_entry e;
            oh.get().convert(e);
            entries_.push_back(e);
            pos += sz;
        }
        open_wal_append();
    }

    void append(const log_entry& e) {
        entries_.push_back(e);
        if (!wal_.is_open())
            open_wal_append();
        msgpack::sbuffer buf;
        msgpack::pack(buf, e);
        write_frame(wal_, buf);
        wal_.flush();
        if (!wal_.good()) {
            detail::logger().warn("file_log_store: write error on {}",
                                  wal_path_);
            wal_.clear();
            open_wal_append();
        }
    }

    void truncate(size_t n) {
        entries_.resize(n);
        rewrite_wal();
    }

    void clear() {
        entries_.clear();
        rewrite_wal();
    }

    void reserve(size_t n) { entries_.reserve(n); }
    size_t size() const { return entries_.size(); }
    bool empty() const { return entries_.empty(); }

    const log_entry& operator[](size_t i) const { return entries_[i]; }
    log_entry& operator[](size_t i) { return entries_[i]; }

    void save_snapshot(const snapshot_entry& s) {
        auto tmp = snap_path_ + ".tmp";
        {
            std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
            msgpack::sbuffer buf;
            msgpack::pack(buf, s);
            const char magic[4] = {'R', 'A', 'F', 'T'};
            f.write(magic, 4);
            auto c = detail::crc32(buf.data(), buf.size());
            char cb[4] = {static_cast<char>(c & 0xff),
                          static_cast<char>((c >> 8) & 0xff),
                          static_cast<char>((c >> 16) & 0xff),
                          static_cast<char>((c >> 24) & 0xff)};
            f.write(cb, 4);
            f.write(buf.data(), buf.size());
        }
        std::filesystem::rename(tmp, snap_path_);
    }

    std::optional<snapshot_entry> load_snapshot() {
        std::ifstream f(snap_path_, std::ios::binary);
        if (!f)
            return std::nullopt;
        std::string data(std::istreambuf_iterator<char>(f), {});
        if (f.bad())
            detail::logger().warn("file_log_store: read error on {}",
                                  snap_path_);
        if (data.empty())
            return std::nullopt;
        if (data.size() < 8 || data[0] != 'R' || data[1] != 'A' ||
            data[2] != 'F' || data[3] != 'T')
            throw std::runtime_error("file_log_store: corrupt snap");
        auto stored = read_le32(data.data() + 4);
        auto actual = detail::crc32(data.data() + 8, data.size() - 8);
        if (actual != stored)
            throw std::runtime_error("file_log_store: corrupt snap");
        auto oh = msgpack::unpack(data.data() + 8, data.size() - 8);
        snapshot_entry s = {};
        oh.get().convert(s);
        return s;
    }

  private:
    void open_wal_append() {
        if (wal_.is_open())
            wal_.close();
        if (wal_path_.empty())
            return;
        wal_.open(wal_path_, std::ios::binary | std::ios::app);
        if (!wal_.is_open())
            throw std::runtime_error("file_log_store: cannot open " +
                                     wal_path_);
    }

    void rewrite_wal() {
        if (wal_.is_open())
            wal_.close();
        if (wal_path_.empty())
            return;
        auto tmp = wal_path_ + ".tmp";
        {
            std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
            if (!f)
                throw std::runtime_error("file_log_store: cannot open " +
                                         tmp);
            for (auto& e : entries_) {
                msgpack::sbuffer buf;
                msgpack::pack(buf, e);
                write_frame(f, buf);
            }
        }
        std::filesystem::rename(tmp, wal_path_);
        open_wal_append();
    }

    static uint32_t read_le32(const char* p) {
        return static_cast<uint32_t>(static_cast<uint8_t>(p[0]) |
                                     (static_cast<uint8_t>(p[1]) << 8) |
                                     (static_cast<uint8_t>(p[2]) << 16) |
                                     (static_cast<uint8_t>(p[3]) << 24));
    }

    static void write_frame(std::ostream& out, const msgpack::sbuffer& buf) {
        uint32_t sz = static_cast<uint32_t>(buf.size());
        auto c = detail::crc32(buf.data(), buf.size());
        char hdr[8];
        hdr[0] = static_cast<char>(sz & 0xff);
        hdr[1] = static_cast<char>((sz >> 8) & 0xff);
        hdr[2] = static_cast<char>((sz >> 16) & 0xff);
        hdr[3] = static_cast<char>((sz >> 24) & 0xff);
        hdr[4] = static_cast<char>(c & 0xff);
        hdr[5] = static_cast<char>((c >> 8) & 0xff);
        hdr[6] = static_cast<char>((c >> 16) & 0xff);
        hdr[7] = static_cast<char>((c >> 24) & 0xff);
        out.write(hdr, 8);
        out.write(buf.data(), buf.size());
    }

    std::vector<log_entry> entries_;
    std::string wal_path_;
    std::string snap_path_;
    std::ofstream wal_;
};

// -------------------------------------------------------
// Transport concept (minimal — 5 methods)
//
// Transport must provide:
//   void send(const message&)
//   void on_message(std::function<void(const message&)>)
//   void listen(uint16_t)
//   void run()
//   void stop()
//
// add_peer / remove_peer are NOT part of the concept.
// -------------------------------------------------------

// -------------------------------------------------------
// node (thread-based, transport-agnostic)
// -------------------------------------------------------
#ifdef SKIFFY_ENABLE_ASIO
class asio_transport;
using default_transport = asio_transport;
inline node_id make_node_id(const std::string& host, uint16_t port);
#else
class channel_transport;
using default_transport = channel_transport;
#endif

template <typename Cmd, typename Transport = default_transport,
          typename LogStore = memory_log_store>
class node {
    // compile-time interface checks
    // clang-format off
    static_assert(std::is_invocable_r_v<
        void, decltype(&LogStore::append), LogStore&, const log_entry&>,
        "LogStore must provide void append(const log_entry&)");
    static_assert(std::is_invocable_r_v<
        size_t, decltype(&LogStore::size), const LogStore&>,
        "LogStore must provide size_t size() const");
    static_assert(std::is_invocable_r_v<
        void, decltype(&LogStore::truncate), LogStore&, size_t>,
        "LogStore must provide void truncate(size_t)");
    static_assert(std::is_invocable_r_v<
        void, decltype(&LogStore::clear), LogStore&>,
        "LogStore must provide void clear()");
    static_assert(std::is_invocable_r_v<
        bool, decltype(&LogStore::empty), const LogStore&>,
        "LogStore must provide bool empty() const");
    static_assert(std::is_invocable_r_v<
        const log_entry&, decltype(static_cast<const log_entry& (LogStore::*)
            (size_t) const>(&LogStore::operator[])), const LogStore&, size_t>,
        "LogStore must provide const log_entry& operator[](size_t) const");
    static_assert(std::is_invocable_r_v<
        log_entry&, decltype(static_cast<log_entry& (LogStore::*)
            (size_t)>(&LogStore::operator[])), LogStore&, size_t>,
        "LogStore must provide log_entry& operator[](size_t)");

    static_assert(std::is_invocable_r_v<
        void, decltype(&Transport::send), Transport&, const message&>,
        "Transport must provide: void send(const message&)");
    static_assert(std::is_invocable_r_v<
        void, decltype(&Transport::on_message), Transport&,
            std::function<void(const message&)>>,
        "Transport must provide: void on_message(callback)");
    static_assert(std::is_invocable_r_v<
        void, decltype(&Transport::listen), Transport&, uint16_t>,
        "Transport must provide: void listen(uint16_t)");
    static_assert(std::is_invocable_r_v<
        void, decltype(&Transport::run), Transport&>,
        "Transport must provide: void run()");
    static_assert(std::is_invocable_r_v<
        void, decltype(&Transport::stop), Transport&>,
        "Transport must provide: void stop()");
    static_assert(std::is_invocable_r_v<
        void, decltype(&Transport::post), Transport&, std::function<void()>>,
        "Transport must provide: void post(fn)");
    // clang-format on

  public:
    node(node_id id, LogStore log_store = LogStore{})
        : id_(id), transport_(id), log_store_(std::move(log_store)),
          srv_(id, {}, transport_, log_store_), mgr_(id),
          rng_(std::random_device{}()) {
        transport_.listen(id_.port_);

        transport_.on_message([this](const message& m) {
            if (m.type >= msg_type::mem_join_req)
                post([this, m] {
                    for (auto& r : mgr_.receive(m))
                        transport_.send(r);
                });
            else
                push_message(m);
        });

        mgr_.on_peer_added([this](node_id pid) {
            post([this, pid] {
                srv_.add_peer(pid);
                last_heard_[pid] = clock_t::now();
            });
        });

        mgr_.on_peer_removed([this](node_id pid) {
            post([this, pid] {
                srv_.remove_peer(pid);
                last_heard_.erase(pid);
            });
        });

        srv_.on_peer_removed([this](node_id pid) {
            mgr_.remove_member(pid);
            last_heard_.erase(pid);
        });

        srv_.on_peer_added([this](node_id pid) {
            for (auto& mi : mgr_.members()) {
                if (mi == pid) {
                    last_heard_[pid] = clock_t::now();
                    break;
                }
            }
        });

        srv_.on_compact([this] {
            snapshot_entry snap;
            snap.index = srv_.snapshot_index();
            snap.term = srv_.snapshot_term();
            {
                msgpack::sbuffer buf;
                msgpack::pack(buf, srv_.applied());
                snap.data = std::string(buf.data(), buf.data() + buf.size());
            }
            log_store_.save_snapshot(snap);
            srv_.prune_applied(applied_up_to_);
            applied_up_to_ = 0;
        });

        srv_.on_entries_dropped([this](std::vector<log_entry> dropped) {
            if (!on_drop_)
                return;
            for (auto& e : dropped) {
                if (e.type != entry_type::data)
                    continue;
                auto oh = msgpack::unpack(e.value.data(), e.value.size());
                Cmd cmd;
                oh.get().convert(cmd);
                on_drop_(cmd);
            }
        });
    }

    node_id id() const { return id_; }
    Transport& transport() { return transport_; }

    void on_apply(std::function<void(const Cmd&)> fn) {
        on_apply_ = std::move(fn);
    }

    void on_drop(std::function<void(const Cmd&)> fn) {
        on_drop_ = std::move(fn);
    }

    void on_leader_changed(std::function<void(node_id)> fn) {
        on_leader_changed_ = std::move(fn);
    }

    void join(node_id bootstrap) {
        post([this, bootstrap] { transport_.send(mgr_.join(bootstrap)); });
    }

#ifdef SKIFFY_ENABLE_ASIO
    void join(const std::string& host_port) {
        auto pos = host_port.rfind(':');
        auto host = host_port.substr(0, pos);
        auto port =
            static_cast<uint16_t>(std::stoi(host_port.substr(pos + 1)));
        join(make_node_id(host, port));
    }
#endif

    void leave() {
        post([this] {
            for (auto& m : mgr_.notify_leave())
                transport_.send(m);
            stop();
        });
    }

    void submit(const Cmd& cmd) {
        msgpack::sbuffer buf;
        msgpack::pack(buf, cmd);
        std::string data{buf.data(), buf.data() + buf.size()};
        {
            std::unique_lock lk(leader_mu_);
            leader_cv_.wait(lk, [this] {
                return last_leader_ != nil_id || !running_.load();
            });
        }
        post([this, d = std::move(data), c = cmd] {
            if (!is_leader()) {
                auto lid = leader_id();
                if (lid != nil_id) {
                    message fwd;
                    fwd.type = msg_type::client_fwd;
                    fwd.from = id_;
                    fwd.to = lid;
                    fwd.payload = d;
                    transport_.send(fwd);
                } else {
                    if (on_drop_)
                        on_drop_(c);
                }
                return;
            }
            srv_.client_request(d);
            for (auto p : srv_.peers())
                srv_.append_entries(p);
        });
    }

    // run() blocks until stop() or leave() is called.
    void run() {
        running_.store(true);
        transport_.run();
        arm_election_timer_();
        std::unique_lock lk(stop_mu_);
        stop_cv_.wait(lk, [this] { return !running_.load(); });
        lk.unlock();
        transport_.stop();
    }

    void stop() {
        if (!running_.exchange(false))
            return;
        leader_cv_.notify_all();
        { std::lock_guard lk(stop_mu_); }
        stop_cv_.notify_all();
    }

    template <typename Rep, typename Period>
    void election_timeout(std::chrono::duration<Rep, Period> mn,
                          std::chrono::duration<Rep, Period> mx) {
        election_timeout_min_ = std::chrono::duration_cast<ms_t>(mn);
        election_timeout_max_ = std::chrono::duration_cast<ms_t>(mx);
    }

    template <typename Rep, typename Period>
    void heartbeat_interval(std::chrono::duration<Rep, Period> d) {
        heartbeat_interval_ = std::chrono::duration_cast<ms_t>(d);
    }

    template <typename Rep, typename Period>
    void removal_timeout(std::chrono::duration<Rep, Period> d) {
        removal_timeout_ = std::chrono::duration_cast<ms_t>(d);
    }

    void compact_threshold(size_t n) {
        post([this, n] { srv_.compact_threshold(n); });
    }

    bool running() const { return running_.load(); }

    bool is_leader() const {
        std::lock_guard lk(leader_mu_);
        return last_leader_ == id_;
    }

    node_id leader_id() const {
        std::lock_guard lk(leader_mu_);
        return last_leader_;
    }

  private:
    using ms_t = std::chrono::milliseconds;
    using clock_t = std::chrono::steady_clock;
    using tp_t = clock_t::time_point;

    void push_message(const message& m) {
        post([this, m] {
            last_heard_[m.from] = clock_t::now();
            srv_.receive(m);
            on_receive_(m);
            fire_applied();
        });
    }

    void post(std::function<void()> fn) { transport_.post(std::move(fn)); }

    void arm_election_timer_() {
        if (!running_.load())
            return;
        std::uniform_int_distribution<int> dist(
            election_timeout_min_.count(), election_timeout_max_.count());
        auto delay = ms_t(dist(rng_));
        auto gen = election_gen_.load();
        transport_.schedule(delay, [this, gen] {
            if (!running_.load())
                return;
            if (election_gen_.load() != gen)
                return;
            do_election_();
        });
    }

    void reset_election_timer_() {
        election_gen_.fetch_add(1, std::memory_order_relaxed);
        arm_election_timer_();
    }

    void arm_heartbeat_timer_() {
        if (!running_.load())
            return;
        transport_.schedule(heartbeat_interval_, [this] {
            if (!running_.load())
                return;
            if (!is_leader())
                return;
            do_heartbeat_();
            arm_heartbeat_timer_();
        });
    }

    void promote_to_leader() {
        election_gen_.fetch_add(1, std::memory_order_relaxed);
        srv_.become_leader();
        leader_cv_.notify_all();
        {
            std::lock_guard lk(leader_mu_);
            last_leader_ = id_;
        }
        if (on_leader_changed_)
            on_leader_changed_(id_);
        do_heartbeat_();
        arm_heartbeat_timer_();
    }

    void do_election_() {
        srv_.timeout();
        for (auto p : srv_.peers())
            srv_.request_vote(p);
        if (srv_.is_quorum(srv_.votes_granted()))
            promote_to_leader();
        else
            reset_election_timer_();
    }

    void do_heartbeat_() {
        for (auto p : srv_.peers())
            srv_.append_entries(p);
        srv_.advance_commit_index();
        fire_applied();
        auto now = clock_t::now();
        std::vector<node_id> stale;
        for (auto p : srv_.peers()) {
            auto it = last_heard_.find(p);
            if (it != last_heard_.end() &&
                now - it->second > removal_timeout_)
                stale.push_back(p);
        }
        for (auto p : stale) {
            srv_.remove_peer(p);
            last_heard_.erase(p);
            for (auto& m : mgr_.broadcast_remove(p))
                transport_.send(m);
        }
    }

    void on_receive_(const message& m) {
        if (m.type == msg_type::append_entries_req) {
            leader_cv_.notify_all();
            node_id prev;
            {
                std::lock_guard lk(leader_mu_);
                prev = last_leader_;
                last_leader_ = m.from;
            }
            if (on_leader_changed_ && prev != m.from)
                on_leader_changed_(m.from);
            reset_election_timer_();
        } else if (m.type == msg_type::request_vote_req) {
            reset_election_timer_();
        }
        if (srv_.state() == detail::server_state::candidate &&
            srv_.is_quorum(srv_.votes_granted()))
            promote_to_leader();
    }

    void fire_applied() {
        const auto& vec = srv_.applied();
        while (applied_up_to_ < vec.size()) {
            const auto& e = vec[applied_up_to_];
            if (on_apply_ && e.type == entry_type::data) {
                auto oh = msgpack::unpack(e.value.data(), e.value.size());
                Cmd cmd;
                oh.get().convert(cmd);
                on_apply_(cmd);
            }
            ++applied_up_to_;
        }
    }

    node_id id_;
    Transport transport_;
    LogStore log_store_;
    detail::server<Transport, LogStore> srv_;
    detail::membership_manager mgr_;
    std::mt19937 rng_;

    std::atomic<bool> running_{false};
    std::mutex stop_mu_;
    std::condition_variable stop_cv_;
    std::atomic<uint64_t> election_gen_{0};

    ms_t election_timeout_min_{150};
    ms_t election_timeout_max_{300};
    ms_t heartbeat_interval_{50};
    ms_t removal_timeout_{30000};

    std::function<void(const Cmd&)> on_apply_;
    std::function<void(const Cmd&)> on_drop_;
    std::function<void(node_id)> on_leader_changed_;
    size_t applied_up_to_ = 0;
    std::map<node_id, tp_t> last_heard_;

    mutable std::mutex leader_mu_;
    node_id last_leader_;
    std::condition_variable leader_cv_;
};

// -------------------------------------------------------
// channel_transport
// single-threaded in-process transport with its own
// worker thread and timer scheduler. Provides the same
// all instances share a static routing table.
// -------------------------------------------------------
struct channel_transport {
    using callback_t = std::function<void(const message&)>;
    using clock_t = std::chrono::steady_clock;
    using tp_t = clock_t::time_point;
    using ns_t = std::chrono::nanoseconds;

    explicit channel_transport(node_id id = {}) : id_(id) {
        if (id_ != nil_id) {
            std::lock_guard lk(s_mu_);
            s_nodes_[id_] = this;
        }
    }

    ~channel_transport() {
        stop();
        if (id_ != nil_id) {
            std::lock_guard lk(s_mu_);
            s_nodes_.erase(id_);
        }
    }

    channel_transport(const channel_transport&) = delete;
    channel_transport& operator=(const channel_transport&) = delete;

    void send(const message& m) {
        channel_transport* dst = nullptr;
        {
            std::lock_guard lk(s_mu_);
            auto it = s_nodes_.find(m.to);
            if (it != s_nodes_.end())
                dst = it->second;
        }
        if (dst)
            dst->post([dst, m] { dst->push(m); });
    }

    void push(const message& m) {
        if (on_message_)
            on_message_(m);
    }

    void on_message(callback_t cb) { on_message_ = std::move(cb); }

    void listen(uint16_t) {}

    void post(std::function<void()> fn) {
        {
            std::lock_guard lk(q_mu_);
            immed_.push_back(std::move(fn));
        }
        q_cv_.notify_one();
    }

    template <typename Rep, typename Period>
    void schedule(std::chrono::duration<Rep, Period> delay,
                  std::function<void()> fn) {
        auto deadline =
            clock_t::now() + std::chrono::duration_cast<ns_t>(delay);
        {
            std::lock_guard lk(q_mu_);
            auto it = std::lower_bound(
                timed_.begin(), timed_.end(), deadline,
                [](const auto& a, const tp_t& b) { return a.first < b; });
            timed_.insert(it, {deadline, std::move(fn)});
        }
        q_cv_.notify_one();
    }

    void run() {
        running_.store(true);
        thread_ = std::thread([this] { worker_(); });
    }

    void stop() {
        if (!running_.exchange(false))
            return;
        q_cv_.notify_all();
        if (thread_.joinable())
            thread_.join();
    }

  private:
    void worker_() {
        while (running_.load()) {
            std::unique_lock lk(q_mu_);
            auto deadline =
                timed_.empty() ? tp_t::max() : timed_.front().first;
            q_cv_.wait_until(lk, deadline, [this] {
                return !running_.load() || !immed_.empty() ||
                    (!timed_.empty() &&
                     timed_.front().first <= clock_t::now());
            });
            std::deque<std::function<void()>> work;
            work.swap(immed_);
            auto now = clock_t::now();
            while (!timed_.empty() && timed_.front().first <= now) {
                work.push_back(std::move(timed_.front().second));
                timed_.pop_front();
            }
            lk.unlock();
            for (auto& f : work)
                f();
        }
    }

    node_id id_;
    callback_t on_message_;
    std::mutex q_mu_;
    std::condition_variable q_cv_;
    std::deque<std::function<void()>> immed_;
    std::deque<std::pair<tp_t, std::function<void()>>> timed_;
    std::thread thread_;
    std::atomic<bool> running_{false};

    static inline std::mutex s_mu_;
    static inline std::map<node_id, channel_transport*> s_nodes_;
};

// unique in-process node_id (loopback, auto-incrementing port).
inline node_id make_node_id() {
    static std::atomic<uint16_t> next{1};
    using arr4 = std::array<uint8_t, 4>;
    return {arr4{127, 0, 0, 1}, next.fetch_add(1)};
}

#ifdef SKIFFY_ENABLE_ASIO
inline asio::ip::tcp::endpoint to_endpoint(const node_id& id) {
    if (id.is_v4()) {
        asio::ip::address_v4::bytes_type b;
        std::copy_n(id.addr_.begin() + 12, 4, b.begin());
        return {asio::ip::address_v4{b}, id.port_};
    }
    asio::ip::address_v6::bytes_type b;
    std::copy(id.addr_.begin(), id.addr_.end(), b.begin());
    return {asio::ip::address_v6{b}, id.port_};
}

inline node_id make_node_id(const asio::ip::tcp::endpoint& ep) {
    const auto& a = ep.address();
    if (a.is_v4())
        return {a.to_v4().to_bytes(), ep.port()};
    return {a.to_v6().to_bytes(), ep.port()};
}

inline node_id make_node_id(const std::string& host, uint16_t port) {
    auto h = host.empty() ? asio::ip::host_name() : host;
    asio::io_context tmp;
    asio::ip::tcp::resolver r(tmp);
    asio::error_code ec;
    auto res = r.resolve(h, std::to_string(port), ec);
    if (!ec && res.begin() != res.end())
        return make_node_id(res.begin()->endpoint());
    return nil_id;
}

class asio_transport {
  public:
    using endpoint_t = asio::ip::tcp::endpoint;
    using callback_t = std::function<void(const message&)>;

    // per-peer persistent connection
    template <typename = void>
    struct peer_conn_t {
        struct s_disc {};
        struct s_conn {};
        struct s_live {};
        struct evt_send {
            std::shared_ptr<std::vector<uint8_t>> buf;
        };
        struct evt_ok {};
        struct evt_fail {};
        struct evt_err {};

        asio::ip::tcp::endpoint ep_;
        asio::io_context& io_;
        node_id owner_;
        std::shared_ptr<asio::ip::tcp::socket> sock_;
        std::deque<std::shared_ptr<std::vector<uint8_t>>> queue_;
        bool writing_ = false;

        void stop() {
            if (sock_)
                sock_->cancel();
        }

        void start_connect() {
            sock_ = std::make_shared<asio::ip::tcp::socket>(io_);
            sock_->async_connect(ep_, [this](asio::error_code ec) {
                if (ec)
                    sm_.process_event(evt_fail{});
                else
                    sm_.process_event(evt_ok{});
            });
        }

        void do_write_next() {
            if (queue_.empty()) {
                writing_ = false;
                return;
            }
            auto buf = queue_.front();
            queue_.pop_front();
            asio::async_write(*sock_, asio::buffer(*buf),
                              [this, buf](asio::error_code ec, size_t) {
                                  if (ec) {
                                      writing_ = false;
                                      sm_.process_event(evt_err{});
                                      return;
                                  }
                                  do_write_next();
                              });
        }

        struct sm_def {
            peer_conn_t* self_;
            explicit sm_def(peer_conn_t* p) : self_(p) {}
            auto operator()() const noexcept {
                namespace sml = boost::sml;
                peer_conn_t* self = self_;
                auto ds = sml::state<s_disc>;
                auto cs = sml::state<s_conn>;
                auto ls = sml::state<s_live>;
                auto on_send_disc = [self](const evt_send& e) noexcept {
                    self->queue_.push_back(e.buf);
                    self->start_connect();
                };
                auto on_send_conn = [self](const evt_send& e) noexcept {
                    self->queue_.push_back(e.buf);
                };
                auto on_ok = [self]() noexcept {
                    self->writing_ = true;
                    self->do_write_next();
                };
                auto on_fail = [self]() noexcept {
                    self->queue_.clear();
                    self->sock_.reset();
                };
                auto on_send_live = [self](const evt_send& e) noexcept {
                    self->queue_.push_back(e.buf);
                    if (!self->writing_) {
                        self->writing_ = true;
                        self->do_write_next();
                    }
                };
                auto on_err = [self]() noexcept {
                    self->writing_ = false;
                    self->sock_.reset();
                };
                return sml::make_transition_table(
                    *ds + sml::event<evt_send> / on_send_disc = cs,
                    cs + sml::event<evt_send> / on_send_conn = cs,
                    cs + sml::event<evt_ok> / on_ok = ls,
                    cs + sml::event<evt_fail> / on_fail = ds,
                    ls + sml::event<evt_send> / on_send_live = ls,
                    ls + sml::event<evt_err> / on_err = ds);
            }
        };

        // sm_ must be last
        boost::sml::sm<sm_def> sm_;

        peer_conn_t(node_id owner, asio::ip::tcp::endpoint ep,
                    asio::io_context& io)
            : ep_(ep), io_(io), owner_(owner), sm_{sm_def{this}} {}

        void send(std::shared_ptr<std::vector<uint8_t>> buf) {
            sm_.process_event(evt_send{buf});
        }
    };
    using peer_conn = peer_conn_t<>;

    explicit asio_transport(node_id self) : self_(self) {}

    void on_message(callback_t cb) { on_message_ = std::move(cb); }

    void remove_peer(node_id id) {
        auto it = peers_.find(id);
        if (it != peers_.end()) {
            it->second->stop();
            asio::post(io_, [p = std::move(it->second)]() noexcept {});
            peers_.erase(it);
        }
    }

    void send(const message& m) {
        auto it = peers_.find(m.to);
        if (it == peers_.end()) {
            auto ep = to_endpoint(m.to);
            auto [ins, ok] = peers_.emplace(
                m.to, std::make_unique<peer_conn>(self_, ep, io_));
            it = ins;
        }
        detail::logger().debug("transport {} send mtype={} to {}", self_,
                               static_cast<int>(m.type), m.to);
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, m);
        auto buf = std::make_shared<std::vector<uint8_t>>(
            reinterpret_cast<const uint8_t*>(sbuf.data()),
            reinterpret_cast<const uint8_t*>(sbuf.data()) + sbuf.size());
        it->second->send(buf);
    }

    void listen(uint16_t port) {
        using tcp = asio::ip::tcp;
        auto any = asio::ip::address_v4::any();
        acceptor_.emplace(io_);
        acceptor_->open(tcp::v4());
        acceptor_->set_option(tcp::acceptor::reuse_address(true));
        acceptor_->bind(tcp::endpoint(any, port));
        acceptor_->listen();
        do_accept();
    }

    uint16_t bound_port() const {
        if (!acceptor_)
            return 0;
        return acceptor_->local_endpoint().port();
    }

    void post(std::function<void()> fn) { asio::post(io_, std::move(fn)); }

    template <typename Rep, typename Period>
    void schedule(std::chrono::duration<Rep, Period> delay,
                  std::function<void()> fn) {
        auto t = std::make_shared<asio::steady_timer>(io_, delay);
        t->async_wait([t, fn = std::move(fn)](asio::error_code ec) {
            if (!ec)
                fn();
        });
    }

    void run() {
        thread_ = std::thread([this] { io_.run(); });
    }

    void stop() {
        io_.stop();
        if (thread_.joinable())
            thread_.join();
    }

  private:
    static constexpr size_t read_buf_size = 4096;

    void do_accept() {
        auto sock = std::make_shared<asio::ip::tcp::socket>(io_);
        acceptor_->async_accept(*sock, [this, sock](asio::error_code ec) {
            if (!ec)
                do_read(sock);
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
                    detail::logger().debug("transport {} read done: {}",
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

    node_id self_;
    asio::io_context io_;
    asio::executor_work_guard<asio::io_context::executor_type> work_{
        io_.get_executor()};
    std::thread thread_;
    std::map<node_id, std::unique_ptr<peer_conn>> peers_;
    callback_t on_message_;
    std::optional<asio::ip::tcp::acceptor> acceptor_;
};

#endif // SKIFFY_ENABLE_ASIO

template <typename Cmd, typename Transport = default_transport,
          typename LogStore = memory_log_store>
auto make_node(node_id id, LogStore log_store = LogStore{}) {
    return node<Cmd, Transport, LogStore>(id, std::move(log_store));
}

} // namespace skiffy

// msgpack pack/convert adaptors for std::optional<T>
namespace msgpack {
MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
    namespace adaptor {

    template <typename T>
    struct pack<std::optional<T>> {
        template <typename Stream>
        packer<Stream>& operator()(packer<Stream>& o,
                                   const std::optional<T>& v) const {
            if (v.has_value())
                o.pack(*v);
            else
                o.pack_nil();
            return o;
        }
    };

    template <typename T>
    struct convert<std::optional<T>> {
        msgpack::object const& operator()(msgpack::object const& o,
                                          std::optional<T>& v) const {
            if (o.type == msgpack::type::NIL)
                v = std::nullopt;
            else
                v = o.as<T>();
            return o;
        }
    };

    } // namespace adaptor
} // MSGPACK_API_VERSION_NAMESPACE(...)
} // namespace msgpack

#endif // SKIFFY_HPP
