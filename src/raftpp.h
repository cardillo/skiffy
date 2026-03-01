#ifndef RAFTPP_H
#define RAFTPP_H

#include <algorithm>
#include <array>
#include <atomic>
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
#include <string_view>
#include <thread>
#include <vector>

#include "boost/sml.hpp"
#include "spdlog/fmt/ostr.h"
#include "spdlog/sinks/null_sink.h"
#include "spdlog/spdlog.h"

#include "asio.hpp"
#include "msgpack.hpp"

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
} // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack

namespace raftpp {

// --- types ---

class server_id {
 public:
    std::array<uint8_t, 16> addr_ = {};
    uint16_t port_ = 0;

    server_id() = default;

    template<size_t N,
             std::enable_if_t<N == 4 || N == 16, int> = 0>
    server_id(const std::array<uint8_t, N>& a, uint16_t p)
        : addr_([&a]() -> std::array<uint8_t, 16> {
                if constexpr (N == 16)
                    return a;
                return {0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0xff, 0xff, a[0], a[1], a[2], a[3]};
            }()),
          port_(p) {}

    template<size_t N>
    server_id(const uint8_t (&a)[N], uint16_t p)
        : server_id(a, p, std::make_index_sequence<N>{}) {}

    explicit server_id(const asio::ip::tcp::endpoint& ep)
        : port_(ep.port()) {
        auto address = ep.address();
        if (address.is_v4()) {
            auto b = address.to_v4().to_bytes();
            addr_ = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                     0xff, 0xff, b[0], b[1], b[2], b[3]};
        } else {
            auto b = address.to_v6().to_bytes();
            std::copy(b.begin(), b.end(), addr_.begin());
        }
    }

    explicit server_id(std::string_view s) {
        auto pos = s.rfind(':');
        if (pos == std::string_view::npos)
            return;
        std::string host(s.substr(0, pos));
        port_ = static_cast<uint16_t>(
            std::stoi(std::string(s.substr(pos + 1))));
        asio::error_code ec;
        auto addr = asio::ip::make_address(host, ec);
        if (ec)
            return;
        if (addr.is_v4()) {
            auto b = addr.to_v4().to_bytes();
            addr_ = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                     0xff, 0xff, b[0], b[1], b[2], b[3]};
        } else {
            auto b = addr.to_v6().to_bytes();
            std::copy(b.begin(), b.end(), addr_.begin());
        }
    }

    constexpr bool is_nil() const {
        return port_ == 0 && addr_ == std::array<uint8_t, 16>{};
    }

    constexpr bool is_v4() const {
        return addr_[10] == 0xff && addr_[11] == 0xff &&
                addr_[0] == 0 && addr_[1] == 0;
    }

    bool operator==(const server_id& o) const {
        return addr_ == o.addr_ && port_ == o.port_;
    }

    bool operator!=(const server_id& o) const {
        return !(*this == o);
    }

    bool operator<(const server_id& o) const {
        if (addr_ != o.addr_)
            return addr_ < o.addr_;
        return port_ < o.port_;
    }

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        pk.pack_bin(18);
        pk.pack_bin_body(
            reinterpret_cast<const char*>(addr_.data()), 16);
        uint8_t pb[2] = {uint8_t(port_ >> 8), uint8_t(port_)};
        pk.pack_bin_body(
            reinterpret_cast<const char*>(pb), 2);
    }

    void msgpack_unpack(msgpack::object const& o) {
        if (o.type != msgpack::type::BIN) return;
        if (o.via.bin.size < 18) return;
        const auto* p =
            reinterpret_cast<const uint8_t*>(o.via.bin.ptr);
        std::copy(p, p + 16, addr_.begin());
        port_ = static_cast<uint16_t>(
            (uint16_t{p[16]} << 8) | p[17]);
    }

  private:
    struct buffer {
        std::array<char, 37> data{};
        size_t len = 0;
        constexpr operator std::string_view() const {
            return { data.data(), len };
        }
    };

    constexpr buffer to_buffer() const {
        buffer s{};
        constexpr char hex[] = "0123456789abcdef";
        auto write = [&](uint8_t b) {
            s.data[s.len++] = hex[(b >> 4) & 0x0f];
            s.data[s.len++] = hex[b & 0x0f];
        };
        if (is_v4()) {
            for (int i = 12; i < 16; ++i)
                write(addr_[i]);
            s.data[s.len++] = '-';
            write(static_cast<uint8_t>(port_ >> 8));
            write(static_cast<uint8_t>(port_ & 0xff));
        } else {
            for (int i = 0; i < 16; ++i) {
                if (i == 4 || i == 6 || i == 8 || i == 10)
                    s.data[s.len++] = '-';
                if (i == 6)
                    write(addr_[i] ^ static_cast<uint8_t>(port_ >> 8));
                else if (i == 7)
                    write(addr_[i] ^ static_cast<uint8_t>(port_ & 0xff));
                else write(addr_[i]);
            }
        }
        return s;
    }

    template<size_t N, size_t... I>
    server_id(const uint8_t (&a)[N], uint16_t p,
              std::index_sequence<I...>)
        : server_id(std::array<uint8_t, N>({a[I]...}), p) {}

    friend std::ostream& operator<<(std::ostream& os, const server_id& sid) {
        return os << static_cast<std::string_view>(sid.to_buffer());
    }
};

} // namespace raftpp

template <>
struct fmt::formatter<raftpp::server_id> : fmt::ostream_formatter {};

namespace raftpp {

using term_t = uint64_t;
using index_t = uint64_t;

const server_id nil_id{};

// --- logging ---

inline std::shared_ptr<spdlog::logger> logger() {
    auto l = spdlog::get("raftpp");
    if (!l) {
        l = std::make_shared<spdlog::logger>(
            "raftpp", std::make_shared<spdlog::sinks::null_sink_mt>());
        spdlog::register_logger(l);
    }
    return l;
}

// --- core enums ---

enum class entry_type : uint8_t {
    data = 0,
    config_joint = 1,
    config_final = 2,
};

enum class server_state { follower, candidate, leader };

enum class protocol_tag : uint8_t {
    raft = 0x01,
    membership = 0x02,
};

enum class msg_type : uint8_t {
    request_vote_req = 0,
    request_vote_resp = 1,
    append_entries_req = 2,
    append_entries_resp = 3,
    install_snapshot_req = 4,
    install_snapshot_resp = 5,
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
    server_id from;
    server_id to;

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
            snapshot_data == o.snapshot_data;
    }
    bool operator!=(const message& o) const { return !(*this == o); }

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        pk.pack_array(16);
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
    }
    void msgpack_unpack(msgpack::object const& o) {
        auto& a = o.via.array;
        type = static_cast<msg_type>(a.ptr[0].as<uint8_t>());
        term = a.ptr[1].as<term_t>();
        from = a.ptr[2].as<server_id>();
        to = a.ptr[3].as<server_id>();
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
    }
};

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

    // no-op persistence interface (mirrors file_log_store)
    void load() {}
    void save_snapshot(const snapshot_t&) {}
    std::optional<snapshot_t> load_snapshot() { return std::nullopt; }

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

    void compact_threshold(size_t n) { compact_threshold_ = n; }

    void on_peer_removed(std::function<void(server_id)> cb) {
        on_peer_removed_ = std::move(cb);
    }

    void on_peer_added(std::function<void(server_id)> cb) {
        on_peer_added_ = std::move(cb);
    }

    void on_compact(std::function<void()> cb) {
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

// --- transport concept ---
//
// Transport must provide:
//   void send(const raftpp::message& m);

// -------------------------------------------------------
// asio_transport + helpers
// -------------------------------------------------------

inline asio::ip::tcp::endpoint
to_endpoint(const server_id& id) {
    if (id.is_v4()) {
        asio::ip::address_v4::bytes_type b;
        std::copy_n(id.addr_.begin() + 12, 4, b.begin());
        return {asio::ip::address_v4{b}, id.port_};
    }
    asio::ip::address_v6::bytes_type b;
    std::copy(id.addr_.begin(), id.addr_.end(), b.begin());
    return {asio::ip::address_v6{b}, id.port_};
}

class asio_transport {
  public:
    using endpoint_t = asio::ip::tcp::endpoint;
    using callback_t = std::function<void(const message&)>;

    // Per-peer persistent connection (disc -> conn -> live).
    // template<typename=void> defers sm<sm_def> instantiation
    // until peer_conn_t<> is complete (SML completeness fix).
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
        server_id owner_;
        std::shared_ptr<asio::ip::tcp::socket> sock_;
        std::deque<std::shared_ptr<std::vector<uint8_t>>> queue_;
        bool writing_ = false;

        void stop() {
            if (sock_)
                sock_->cancel();
        }

        // Defined before sm_def so the lambdas inside
        // sm_def::operator()() can reference them by name.
        // Bodies reference sm_ (declared later) — valid because
        // template member bodies are lazily instantiated and see
        // the full class scope.
        void start_connect() {
            sock_ = std::make_shared<asio::ip::tcp::socket>(io_);
            sock_->async_connect(ep_, [this](asio::error_code ec) {
                if (ec)
                    sm_.process_event(evt_fail{});
                else
                    sm_.process_event(evt_ok{});
            });
        }

        void send_tag_byte() {
            static const std::array<uint8_t, 1> tag{0x01};
            asio::async_write(*sock_, asio::buffer(tag),
                              [this](asio::error_code ec, size_t) {
                                  if (ec) {
                                      sm_.process_event(evt_err{});
                                      return;
                                  }
                                  do_write_next();
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
                    self->send_tag_byte();
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

        peer_conn_t(server_id owner, asio::ip::tcp::endpoint ep,
                    asio::io_context& io)
            : ep_(ep), io_(io), owner_(owner), sm_{sm_def{this}} {}

        void send(std::shared_ptr<std::vector<uint8_t>> buf) {
            sm_.process_event(evt_send{buf});
        }
    };
    using peer_conn = peer_conn_t<>;

    asio_transport(server_id self, asio::io_context& io)
        : self_(self), io_(io) {}

    // Must be called before run() — not thread-safe.
    void on_message(callback_t cb) { on_message_ = std::move(cb); }

    void add_peer(server_id id) {
        auto ep = to_endpoint(id);
        peers_.emplace(id, std::make_unique<peer_conn>(self_, ep, io_));
        logger()->debug("transport {} add_peer {} at {}:{}", self_, id,
                        ep.address().to_string(), ep.port());
    }

    void remove_peer(server_id id) {
        auto it = peers_.find(id);
        if (it != peers_.end()) {
            it->second->stop();
            // defer destruction so operation_aborted callbacks
            // fire before the object is freed
            asio::post(io_, [p = std::move(it->second)]() noexcept {});
            peers_.erase(it);
        }
    }

    // Called by cluster_node after reading tag byte 0x01.
    void
    accept_connection(const std::shared_ptr<asio::ip::tcp::socket>& sock) {
        do_read(sock);
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
        it->second->send(buf);
    }

    using socket_t = asio::ip::tcp::socket;
    using mem_sock_cb_t =
        std::function<void(std::shared_ptr<socket_t>)>;

    void listen(uint16_t port, mem_sock_cb_t on_membership_sock) {
        on_membership_sock_ = std::move(on_membership_sock);
        using tcp = asio::ip::tcp;
        auto any = asio::ip::address_v4::any();
        acceptor_.emplace(io_);
        acceptor_->open(tcp::v4());
        acceptor_->set_option(tcp::acceptor::reuse_address(true));
        acceptor_->bind(tcp::endpoint(any, port));
        acceptor_->listen();
        do_accept();
    }

  private:
    static constexpr size_t read_buf_size = 4096;

    void do_accept() {
        auto sock = std::make_shared<asio::ip::tcp::socket>(io_);
        acceptor_->async_accept(*sock,
                                [this, sock](asio::error_code ec) {
            if (!ec)
                do_route(sock);
            do_accept();
        });
    }

    void do_route(const std::shared_ptr<asio::ip::tcp::socket>& sock) {
        auto tag = std::make_shared<std::array<uint8_t, 1>>();
        asio::async_read(*sock, asio::buffer(*tag),
                         [this, sock, tag](asio::error_code ec,
                                          size_t) {
            if (ec)
                return;
            if (static_cast<protocol_tag>((*tag)[0]) ==
                protocol_tag::raft)
                accept_connection(sock);
            else if (static_cast<protocol_tag>((*tag)[0]) ==
                     protocol_tag::membership)
                on_membership_sock_(sock);
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

    server_id self_;
    asio::io_context& io_;
    std::map<server_id, std::unique_ptr<peer_conn>> peers_;
    callback_t on_message_;
    std::optional<asio::ip::tcp::acceptor> acceptor_;
    mem_sock_cb_t on_membership_sock_;
};

// -------------------------------------------------------
// membership
// -------------------------------------------------------

struct member_info {
    server_id addr; // "host:raft_port"

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        pk.pack_array(1);
        pk.pack(addr);
    }

    void msgpack_unpack(msgpack::object const& o) {
        addr = o.via.array.ptr[0].as<server_id>();
    }

    asio::ip::tcp::endpoint endpoint() const {
        return to_endpoint(addr);
    }

    std::string host() const {
        if (addr.is_v4()) {
            char buf[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, addr.addr_.data() + 12, buf,
                      sizeof(buf));
            return std::string(buf);
        } else {
            char buf[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET6, addr.addr_.data(), buf, sizeof(buf));
            return std::string(buf);
        }
    }

    uint16_t raft_port() const { return addr.port_; }
};

enum class mem_msg_type : uint8_t {
    join_req = 1,
    join_resp = 2,
    announce = 3,
    remove = 4,
};

struct mem_message {
    mem_msg_type type;
    std::optional<server_id> joiner_addr;
    std::optional<std::vector<member_info>> members;

    template <typename Packer>
    void msgpack_pack(Packer& pk) const {
        pk.pack_array(3);
        pk.pack(static_cast<uint8_t>(type));
        pk.pack(joiner_addr);
        pk.pack(members);
    }

    void msgpack_unpack(msgpack::object const& o) {
        auto& a = o.via.array;
        type = static_cast<mem_msg_type>(a.ptr[0].as<uint8_t>());
        a.ptr[1].convert(joiner_addr);
        a.ptr[2].convert(members);
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
        : self_(self), io_(io), transport_(transport) {}

    void self_info(const std::string& host, uint16_t raft_port) {
        member_info mi;
        mi.addr = server_id(host + ":" + std::to_string(raft_port));
        members_.push_back(mi);
    }

    server_id self_addr() const { return members_[0].addr; }

    // Called by cluster_node after reading tag byte 0x02.
    void
    accept_connection(const std::shared_ptr<asio::ip::tcp::socket>& sock) {
        handle_connection(sock);
    }

    void join(const endpoint_t& bootstrap_ep) {
        logger()->info("server {} joining via {}:{}", self_,
                       bootstrap_ep.address().to_string(),
                       bootstrap_ep.port());

        asio::ip::tcp::socket sock(io_);
        sock.connect(bootstrap_ep);

        // write membership tag byte
        const uint8_t tag = static_cast<uint8_t>(protocol_tag::membership);
        asio::write(sock, asio::buffer(&tag, 1));

        mem_message req;
        req.type = mem_msg_type::join_req;
        req.joiner_addr = self_;

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
                if (mi.addr == self_)
                    continue;
                transport_.add_peer(mi.addr);
                if (on_peer_added_) {
                    on_peer_added_(mi.addr, mi.endpoint());
                }
                logger()->info("server {} peer {} added", self_, mi.addr);

                asio::ip::tcp::socket asock(io_);
                asock.connect(mi.endpoint());

                const uint8_t atag =
                    static_cast<uint8_t>(protocol_tag::membership);
                asio::write(asock, asio::buffer(&atag, 1));

                mem_message ann;
                ann.type = mem_msg_type::announce;
                ann.joiner_addr = self_;

                msgpack::sbuffer ann_buf;
                msgpack::pack(ann_buf, ann);
                asio::write(asock,
                            asio::buffer(ann_buf.data(), ann_buf.size()));
            }
        }
    }

    // Must be called before run() — not thread-safe.
    void on_peer_added(on_peer_added_t cb) {
        on_peer_added_ = std::move(cb);
    }

    // Must be called before run() — not thread-safe.
    void on_peer_removed(on_peer_removed_t cb) {
        on_peer_removed_ = std::move(cb);
    }

    const std::vector<member_info>& members() const { return members_; }

    void remove_member(server_id id) {
        members_.erase(
            std::remove_if(members_.begin(), members_.end(),
                           [id](const member_info& m) {
                               return m.addr == id;
                           }),
            members_.end());
    }

    void notify_leave() {
        mem_message msg;
        msg.type = mem_msg_type::remove;
        msg.joiner_addr = self_;
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, msg);
        asio::io_context tmp;
        for (auto& mi : members_) {
            if (mi.addr == self_)
                continue;
            asio::ip::tcp::socket s(tmp);
            asio::error_code ec;
            s.connect(mi.endpoint(), ec);
            if (!ec) {
                const uint8_t tag =
                    static_cast<uint8_t>(protocol_tag::membership);
                asio::write(s, asio::buffer(&tag, 1), ec);
                if (!ec)
                    asio::write(s, asio::buffer(sbuf.data(), sbuf.size()),
                                ec);
            }
        }
    }

    void broadcast_remove(server_id failed_id) {
        remove_member(failed_id);
        mem_message msg;
        msg.type = mem_msg_type::remove;
        msg.joiner_addr = failed_id;
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, msg);
        auto buf = std::make_shared<msgpack::sbuffer>(std::move(sbuf));
        for (auto& mi : members_) {
            if (mi.addr == self_)
                continue;
            auto s = std::make_shared<asio::ip::tcp::socket>(io_);
            s->async_connect(mi.endpoint(), [s, buf](asio::error_code ec) {
                if (ec)
                    return;
                auto tag = std::make_shared<std::array<uint8_t, 1>>(
                    std::array<uint8_t, 1>{0x02});
                asio::async_write(
                    *s, asio::buffer(*tag),
                    [s, buf, tag](asio::error_code ec2, size_t) {
                        if (ec2)
                            return;
                        asio::async_write(
                            *s, asio::buffer(buf->data(), buf->size()),
                            [s, buf](asio::error_code, size_t) {});
                    });
            });
        }
    }

  private:
    void
    handle_connection(const std::shared_ptr<asio::ip::tcp::socket>& sock) {
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

    void do_add_member(const mem_message& msg) {
        if (!msg.joiner_addr)
            return;
        member_info mi;
        mi.addr = *msg.joiner_addr;
        for (auto& x : members_)
            if (x.addr == mi.addr)
                return;
        members_.push_back(mi);
        transport_.add_peer(mi.addr);
        if (on_peer_added_)
            on_peer_added_(mi.addr, mi.endpoint());
        logger()->info("server {} peer {} added", self_, mi.addr);
    }

    void
    handle_mem_message(const std::shared_ptr<asio::ip::tcp::socket>& sock,
                       const mem_message& msg) {
        if (msg.type == mem_msg_type::join_req) {
            do_add_member(msg);
            mem_message resp;
            resp.type = mem_msg_type::join_resp;
            resp.members = members_;
            msgpack::sbuffer resp_sbuf;
            msgpack::pack(resp_sbuf, resp);
            auto buf =
                std::make_shared<msgpack::sbuffer>(std::move(resp_sbuf));
            asio::async_write(*sock, asio::buffer(buf->data(), buf->size()),
                              [sock, buf](asio::error_code, size_t) {});
        } else if (msg.type == mem_msg_type::announce) {
            do_add_member(msg);
        } else if (msg.type == mem_msg_type::remove) {
            if (auto gone = msg.joiner_addr) {
                remove_member(*gone);
                if (on_peer_removed_)
                    on_peer_removed_(*gone);
            }
        }
    }

    server_id self_;
    asio::io_context& io_;
    asio_transport& transport_;
    std::vector<member_info> members_;
    on_peer_added_t on_peer_added_;
    on_peer_removed_t on_peer_removed_;
};

// -------------------------------------------------------
// cluster_node
// -------------------------------------------------------

template <typename Cmd, typename LogStore = file_log_store>
class cluster_node {
  public:
    using server_t = server<asio_transport, LogStore, log_state_machine>;

    static std::string advertise_host_(
            const std::string& host, uint16_t port) {
        std::string h =
            host.empty() ? asio::ip::host_name() : host;
        asio::io_context tmp;
        asio::ip::tcp::resolver r(tmp);
        asio::error_code ec;
        auto res = r.resolve(h, std::to_string(port), ec);
        if (!ec && res.begin() != res.end())
            return res.begin()->endpoint().address().to_string();
        return h;
    }

    cluster_node(const std::string& host, uint16_t port,
                 const std::string& data_dir = "data")
        : port_(port),
          log_store_(init_store_(data_dir,
                                 advertise_host_(host, port) +
                                 ":" + std::to_string(port))),
          transport_(server_id(advertise_host_(host, port) +
                               ":" + std::to_string(port)), io_),
          mgr_(server_id(advertise_host_(host, port) +
                         ":" + std::to_string(port)),
               io_, transport_),
          srv_(server_id(advertise_host_(host, port) +
                         ":" + std::to_string(port)), {},
               transport_, log_store_, sm_),
          election_timer_(io_), heartbeat_timer_(io_),
          rng_(std::random_device{}()), election_dist_(150, 300) {
        id_ = server_id(advertise_host_(host, port) +
                        ":" + std::to_string(port));

        log_store_.load();
        auto snap = log_store_.load_snapshot();
        if (snap)
            sm_.install(snap->data);

        transport_.listen(port_,
            [this](auto sock) { mgr_.accept_connection(sock); });

        mgr_.self_info(advertise_host_(host, port_), port_);

        transport_.on_message([this](const message& m) {
            srv_.receive(m);
            on_receive_(m);
        });

        mgr_.on_peer_added(
            [this](server_id pid, const asio::ip::tcp::endpoint&) {
                asio::post(io_, [this, pid] {
                    srv_.add_peer(pid);
                    last_heard_[pid] = std::chrono::steady_clock::now();
                });
            });

        mgr_.on_peer_removed([this](server_id pid) {
            srv_.remove_peer(pid);
            transport_.remove_peer(pid);
            last_heard_.erase(pid);
        });

        srv_.on_peer_removed([this](server_id pid) {
            transport_.remove_peer(pid);
            mgr_.remove_member(pid);
            last_heard_.erase(pid);
        });

        srv_.on_peer_added([this](server_id pid) {
            for (auto& mi : mgr_.members()) {
                if (mi.addr == pid) {
                    transport_.add_peer(pid);
                    last_heard_[pid] = std::chrono::steady_clock::now();
                    break;
                }
            }
        });

        srv_.on_compact([this] {
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
        uint16_t port =
            static_cast<uint16_t>(std::stoi(addr.substr(pos + 1)));
        asio::ip::tcp::resolver resolver(io_);
        auto results = resolver.resolve(host, std::to_string(port));
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

    void submit(Cmd cmd) {
        msgpack::sbuffer buf;
        msgpack::pack(buf, cmd);
        std::string data{buf.data(), buf.data() + buf.size()};
        while (running() && !has_leader_.load())
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        asio::post(io_, [this, d = std::move(data)] {
            if (!is_leader())
                return;
            srv_.client_request(d);
            for (auto p : srv_.peers())
                srv_.append_entries(p);
        });
    }

    // Must be called before run() — not thread-safe.
    void on_apply(std::function<void(const Cmd&)> fn) {
        on_apply_ = std::move(fn);
    }

    server_state state() const { return srv_.state(); }

    bool is_leader() const { return srv_.state() == server_state::leader; }

    void compact_threshold(size_t n) {
        srv_.compact_threshold(n);
    }

    server_id leader_id() const {
        std::lock_guard lk(leader_addr_mu_);
        return last_leader_addr_;
    }

    bool running() const { return !io_.stopped(); }

    size_t peer_count() const { return srv_.peers().size(); }

  private:
    static LogStore init_store_(const std::string& data_dir,
                                const std::string& id_str) {
        if constexpr (std::is_constructible_v<LogStore,
                                             const std::string&>) {
            std::string safe_id = id_str;
            for (auto& c : safe_id)
                if (c == ':')
                    c = '_';
            auto path = data_dir + "/" + safe_id;
            std::filesystem::create_directories(path);
            return LogStore(path + "/raft");
        } else {
            return LogStore{};
        }
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

    void promote_to_leader() {
        srv_.become_leader();
        has_leader_.store(true);
        {
            std::lock_guard lk(leader_addr_mu_);
            last_leader_addr_ = id_;
        }
        start_heartbeat_timer();
        do_heartbeat();
    }

    void do_election() {
        srv_.timeout();
        for (auto p : srv_.peers())
            srv_.request_vote(p);
        if (srv_.is_quorum(srv_.votes_granted()))
            promote_to_leader();
        else
            reset_election_timer();
    }

    void on_receive_(const message& m) {
        last_heard_[m.from] = std::chrono::steady_clock::now();
        if (m.type == msg_type::append_entries_req ||
            m.type == msg_type::request_vote_req) {
            reset_election_timer();
        }
        if (m.type == msg_type::append_entries_req) {
            has_leader_.store(true);
            {
                std::lock_guard lk(leader_addr_mu_);
                last_leader_addr_ = m.from;
            }
        }
        if (srv_.state() == server_state::candidate &&
            srv_.is_quorum(srv_.votes_granted())) {
            promote_to_leader();
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
                auto oh = msgpack::unpack(e.value.data(), e.value.size());
                Cmd cmd;
                oh.get().convert(cmd);
                on_apply_(cmd);
            }
            ++applied_up_to_;
        }
    }

    server_id id_;
    uint16_t port_;
    asio::io_context io_;
    LogStore log_store_;
    log_state_machine sm_;
    asio_transport transport_;
    membership_manager mgr_;
    server_t srv_;
    asio::steady_timer election_timer_;
    asio::steady_timer heartbeat_timer_;
    std::mt19937 rng_;
    std::uniform_int_distribution<int> election_dist_;
    std::function<void(const Cmd&)> on_apply_;
    size_t applied_up_to_ = 0;
    std::map<server_id, std::chrono::steady_clock::time_point> last_heard_;
    std::atomic<bool> has_leader_{false};
    mutable std::mutex leader_addr_mu_;
    server_id last_leader_addr_;
    static constexpr auto removal_timeout_ = std::chrono::seconds(30);
};

} // namespace raftpp

#endif // RAFTPP_H
