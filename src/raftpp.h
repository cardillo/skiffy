#ifndef RAFTPP_H
#define RAFTPP_H

#include <algorithm>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace raftpp {

// --- types ---

using server_id = uint64_t;
using term_t = uint64_t;
using index_t = uint64_t;

constexpr server_id nil_id = 0;

enum class server_state { follower, candidate, leader };

enum class msg_type {
    request_vote_req,
    request_vote_resp,
    append_entries_req,
    append_entries_resp
};

struct log_entry {
    term_t term = 0;
    std::string value;

    bool operator==(const log_entry& o) const {
        return term == o.term && value == o.value;
    }
    bool operator!=(const log_entry& o) const {
        return !(*this == o);
    }
};

struct message {
    msg_type mtype;
    term_t mterm = 0;
    server_id msource = 0;
    server_id mdest = 0;

    // request_vote_req fields
    std::optional<term_t> mlast_log_term;
    std::optional<index_t> mlast_log_index;

    // request_vote_resp fields
    std::optional<bool> mvote_granted;

    // append_entries_req fields
    std::optional<index_t> mprev_log_index;
    std::optional<term_t> mprev_log_term;
    std::optional<std::vector<log_entry>> mentries;
    std::optional<index_t> mcommit_index;

    // append_entries_resp fields
    std::optional<bool> msuccess;
    std::optional<index_t> mmatch_index;

    bool operator==(const message& o) const {
        return mtype == o.mtype
            && mterm == o.mterm
            && msource == o.msource
            && mdest == o.mdest
            && mlast_log_term == o.mlast_log_term
            && mlast_log_index == o.mlast_log_index
            && mvote_granted == o.mvote_granted
            && mprev_log_index == o.mprev_log_index
            && mprev_log_term == o.mprev_log_term
            && mentries == o.mentries
            && mcommit_index == o.mcommit_index
            && msuccess == o.msuccess
            && mmatch_index == o.mmatch_index;
    }
    bool operator!=(const message& o) const {
        return !(*this == o);
    }
};

// --- transport ---

// Transport concept:
//
// A Transport must provide:
//   void send(const raftpp::message& m);
//
// The server calls transport_.send(m) whenever it needs
// to emit a message to another server.

struct transport_tag {};

// --- memory_transport ---

struct memory_transport {
    std::vector<message> sent;

    void send(const message& m) { sent.push_back(m); }
    void clear() { sent.clear(); }
};

// --- server ---

template <typename Transport>
class server {
public:
    server(server_id self,
           std::set<server_id> peers,
           Transport& transport)
        : id_(self)
        , peers_(std::move(peers))
        , transport_(transport)
    {
        // TLA+ Init: currentTerm=1, state=Follower, votedFor=Nil
        current_term_ = 1;
        state_ = server_state::follower;
        voted_for_ = nil_id;

        // logVars
        commit_index_ = 0;

        // candidateVars
        votes_responded_.clear();
        votes_granted_.clear();

        // leaderVars: nextIndex[j]=1, matchIndex[j]=0
        init_leader_vars();
    }

    // --- TLA+ actions (public API) ---

    void restart() {
        state_ = server_state::follower;
        votes_responded_.clear();
        votes_granted_.clear();
        init_leader_vars();
        commit_index_ = 0;
        // currentTerm, votedFor, log are preserved
    }

    void timeout() {
        if (state_ != server_state::follower
            && state_ != server_state::candidate) {
            return;
        }
        state_ = server_state::candidate;
        current_term_++;
        voted_for_ = nil_id;
        votes_responded_.clear();
        votes_granted_.clear();
    }

    void request_vote(server_id j) {
        if (state_ != server_state::candidate) return;
        if (votes_responded_.count(j)) return;

        message m;
        m.mtype = msg_type::request_vote_req;
        m.mterm = current_term_;
        m.mlast_log_term = last_term();
        m.mlast_log_index = log_.size();
        m.msource = id_;
        m.mdest = j;
        transport_.send(m);
    }

    void become_leader() {
        if (state_ != server_state::candidate) return;
        if (!is_quorum(votes_granted_)) return;

        state_ = server_state::leader;
        for (auto& p : all_servers()) {
            next_index_[p] =
                static_cast<index_t>(log_.size()) + 1;
            match_index_[p] = 0;
        }
    }

    void client_request(std::string v) {
        if (state_ != server_state::leader) return;
        log_entry entry;
        entry.term = current_term_;
        entry.value = std::move(v);
        log_.push_back(std::move(entry));
    }

    void append_entries(server_id j) {
        if (j == id_) return;
        if (state_ != server_state::leader) return;

        index_t prev_log_index = next_index_[j] - 1;
        term_t prev_log_term = 0;
        if (prev_log_index > 0) {
            prev_log_term = log_[prev_log_index - 1].term;
        }

        index_t last_entry = std::min(
            static_cast<index_t>(log_.size()),
            next_index_[j]);

        std::vector<log_entry> entries;
        if (next_index_[j] <= log_.size()) {
            entries.push_back(log_[next_index_[j] - 1]);
        }

        index_t commit =
            std::min(commit_index_, last_entry);

        message m;
        m.mtype = msg_type::append_entries_req;
        m.mterm = current_term_;
        m.mprev_log_index = prev_log_index;
        m.mprev_log_term = prev_log_term;
        m.mentries = std::move(entries);
        m.mcommit_index = commit;
        m.msource = id_;
        m.mdest = j;
        transport_.send(m);
    }

    void advance_commit_index() {
        if (state_ != server_state::leader) return;

        index_t new_commit = commit_index_;
        for (index_t idx = log_.size();
             idx >= 1; --idx) {
            if (log_[idx - 1].term != current_term_) {
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
    }

    void receive(const message& m) {
        if (m.mdest != id_) return;

        if (m.mterm > current_term_) {
            update_term(m);
        }

        switch (m.mtype) {
        case msg_type::request_vote_req:
            handle_request_vote_request(m);
            break;
        case msg_type::request_vote_resp:
            if (!drop_stale_response(m)) {
                handle_request_vote_response(m);
            }
            break;
        case msg_type::append_entries_req:
            handle_append_entries_request(m);
            break;
        case msg_type::append_entries_resp:
            if (!drop_stale_response(m)) {
                handle_append_entries_response(m);
            }
            break;
        }
    }

    // --- accessors for testing ---

    server_id id() const { return id_; }
    term_t current_term() const {
        return current_term_;
    }
    server_state state() const { return state_; }
    server_id voted_for() const {
        return voted_for_;
    }
    index_t commit_index() const {
        return commit_index_;
    }

    const std::vector<log_entry>& log() const {
        return log_;
    }
    const std::set<server_id>&
    votes_responded() const {
        return votes_responded_;
    }
    const std::set<server_id>&
    votes_granted() const {
        return votes_granted_;
    }
    index_t next_index_for(server_id j) const {
        return next_index_.at(j);
    }
    index_t match_index_for(server_id j) const {
        return match_index_.at(j);
    }

    const std::set<server_id>& peers() const {
        return peers_;
    }

    term_t last_term() const {
        if (log_.empty()) return 0;
        return log_.back().term;
    }

    bool is_quorum(
        const std::set<server_id>& s) const {
        size_t cluster = peers_.size() + 1;
        return s.size() * 2 > cluster;
    }

private:
    std::set<server_id> all_servers() const {
        std::set<server_id> all = peers_;
        all.insert(id_);
        return all;
    }

    void init_leader_vars() {
        for (auto& p : all_servers()) {
            next_index_[p] = 1;
            match_index_[p] = 0;
        }
    }

    void update_term(const message& m) {
        current_term_ = m.mterm;
        state_ = server_state::follower;
        voted_for_ = nil_id;
    }

    bool drop_stale_response(
        const message& m) const {
        return m.mterm < current_term_;
    }

    void handle_request_vote_request(
        const message& m) {
        server_id j = m.msource;
        term_t m_last_log_term =
            m.mlast_log_term.value_or(0);
        index_t m_last_log_index =
            m.mlast_log_index.value_or(0);

        bool log_ok =
            m_last_log_term > last_term()
            || (m_last_log_term == last_term()
                && m_last_log_index >= log_.size());

        bool grant =
            m.mterm == current_term_
            && log_ok
            && (voted_for_ == nil_id
                || voted_for_ == j);

        if (grant) {
            voted_for_ = j;
        }

        message resp;
        resp.mtype = msg_type::request_vote_resp;
        resp.mterm = current_term_;
        resp.mvote_granted = grant;
        resp.msource = id_;
        resp.mdest = j;
        transport_.send(resp);
    }

    void handle_request_vote_response(
        const message& m) {
        if (m.mterm != current_term_) return;

        server_id j = m.msource;
        votes_responded_.insert(j);

        if (m.mvote_granted.value_or(false)) {
            votes_granted_.insert(j);
        }
    }

    void handle_append_entries_request(
        const message& m) {
        server_id j = m.msource;
        index_t prev_idx =
            m.mprev_log_index.value_or(0);
        term_t prev_term =
            m.mprev_log_term.value_or(0);
        const auto& entries =
            m.mentries.value_or(
                std::vector<log_entry>{});
        index_t m_commit =
            m.mcommit_index.value_or(0);

        bool log_ok =
            prev_idx == 0
            || (prev_idx > 0
                && prev_idx <= log_.size()
                && log_[prev_idx - 1].term
                   == prev_term);

        // reject
        if (m.mterm < current_term_
            || (m.mterm == current_term_
                && state_
                   == server_state::follower
                && !log_ok)) {
            message resp;
            resp.mtype =
                msg_type::append_entries_resp;
            resp.mterm = current_term_;
            resp.msuccess = false;
            resp.mmatch_index = 0;
            resp.msource = id_;
            resp.mdest = j;
            transport_.send(resp);
            return;
        }

        // candidate steps down
        if (m.mterm == current_term_
            && state_
               == server_state::candidate) {
            state_ = server_state::follower;
            return;
        }

        // accept (state==follower, mterm==currentTerm,
        // logOk)
        index_t index = prev_idx + 1;

        if (entries.empty()
            || (log_.size() >= index
                && log_[index - 1].term
                   == entries[0].term)) {
            // already have the entry or empty
            commit_index_ = m_commit;
            message resp;
            resp.mtype =
                msg_type::append_entries_resp;
            resp.mterm = current_term_;
            resp.msuccess = true;
            resp.mmatch_index =
                prev_idx + entries.size();
            resp.msource = id_;
            resp.mdest = j;
            transport_.send(resp);
            return;
        }

        if (!entries.empty()
            && log_.size() >= index
            && log_[index - 1].term
               != entries[0].term) {
            // conflict: truncate
            log_.resize(index - 1);
            return;
        }

        if (!entries.empty()
            && log_.size() == prev_idx) {
            // append
            log_.push_back(entries[0]);
            return;
        }
    }

    void handle_append_entries_response(
        const message& m) {
        if (m.mterm != current_term_) return;

        server_id j = m.msource;
        if (m.msuccess.value_or(false)) {
            index_t mi =
                m.mmatch_index.value_or(0);
            next_index_[j] = mi + 1;
            match_index_[j] = mi;
        } else {
            if (next_index_[j] > 1) {
                next_index_[j]--;
            }
        }
    }

    // --- state ---
    server_id id_;
    std::set<server_id> peers_;
    Transport& transport_;

    // serverVars
    term_t current_term_ = 1;
    server_state state_ = server_state::follower;
    server_id voted_for_ = nil_id;

    // logVars
    std::vector<log_entry> log_;
    index_t commit_index_ = 0;

    // candidateVars
    std::set<server_id> votes_responded_;
    std::set<server_id> votes_granted_;

    // leaderVars
    std::map<server_id, index_t> next_index_;
    std::map<server_id, index_t> match_index_;
};

} // namespace raftpp

#endif // RAFTPP_H
