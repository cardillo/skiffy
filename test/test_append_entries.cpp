#include "doctest/doctest.h"
#include "raftpp.h"

using namespace raftpp;

static server<memory_transport>
make_leader(memory_transport& t) {
    server<memory_transport> s(1, {2, 3}, t);
    s.timeout();
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = s.current_term();
    v.vote_granted = true;
    v.to = 1;
    v.from = 2; s.receive(v);
    v.from = 3; s.receive(v);
    s.become_leader();
    t.clear();
    return s;
}

TEST_CASE("leader sends empty AppendEntries") {
    memory_transport t;
    auto s = make_leader(t);

    s.append_entries(2);

    REQUIRE(t.sent.size() == 1);
    auto& m = t.sent[0];
    CHECK(m.type == msg_type::append_entries_req);
    CHECK(m.term == s.current_term());
    CHECK(m.from == 1);
    CHECK(m.to == 2);
    CHECK(m.prev_log_index.value() == 0);
    CHECK(m.prev_log_term.value() == 0);
    CHECK(m.entries.value().empty());
    CHECK(m.commit_index.value() == 0);
}

TEST_CASE("leader sends entry in AppendEntries") {
    memory_transport t;
    auto s = make_leader(t);
    s.client_request("x");

    s.append_entries(2);

    REQUIRE(t.sent.size() == 1);
    auto& m = t.sent[0];
    REQUIRE(m.entries.value().size() == 1);
    CHECK(m.entries.value()[0].value == "x");
    CHECK(m.prev_log_index.value() == 0);
}

TEST_CASE("append_entries to self is no-op") {
    memory_transport t;
    auto s = make_leader(t);
    s.append_entries(1);
    CHECK(t.sent.empty());
}

TEST_CASE("follower accepts AppendEntries") {
    memory_transport t_follower;
    server follower(2, {1, 3}, t_follower);

    // append_entries with one entry
    message ae;
    ae.type = msg_type::append_entries_req;
    ae.term = 1;
    ae.prev_log_index = 0;
    ae.prev_log_term = 0;
    ae.entries = std::vector<log_entry>{
        {1, entry_type::data, "x"}};
    ae.commit_index = 0;
    ae.from = 1;
    ae.to = 2;

    follower.receive(ae);
    CHECK(follower.log().size() == 1);
    CHECK(follower.log()[0].value == "x");
}

TEST_CASE("follower rejects AppendEntries with bad prev") {
    memory_transport t_follower;
    server follower(2, {1, 3}, t_follower);

    // AE with prevLogIndex=1 but follower has empty log
    message ae;
    ae.type = msg_type::append_entries_req;
    ae.term = 1;
    ae.prev_log_index = 1;
    ae.prev_log_term = 1;
    ae.entries = std::vector<log_entry>{
        {1, entry_type::data, "x"}};
    ae.commit_index = 0;
    ae.from = 1;
    ae.to = 2;

    follower.receive(ae);

    REQUIRE(t_follower.sent.size() == 1);
    CHECK(t_follower.sent[0].success.value() == false);
    CHECK(follower.log().empty());
}

TEST_CASE("follower truncates conflicting entries") {
    memory_transport t_f;
    server follower(2, {1, 3}, t_f);

    // give follower a log entry at term 1
    message ae1;
    ae1.type = msg_type::append_entries_req;
    ae1.term = 1;
    ae1.prev_log_index = 0;
    ae1.prev_log_term = 0;
    ae1.entries = std::vector<log_entry>{
        {1, entry_type::data, "old"}};
    ae1.commit_index = 0;
    ae1.from = 1;
    ae1.to = 2;
    follower.receive(ae1);
    CHECK(follower.log().size() == 1);

    t_f.clear();

    // new leader sends entry at index 1 with different term
    message ae2;
    ae2.type = msg_type::append_entries_req;
    ae2.term = 2;
    ae2.prev_log_index = 0;
    ae2.prev_log_term = 0;
    ae2.entries = std::vector<log_entry>{
        {2, entry_type::data, "new"}};
    ae2.commit_index = 0;
    ae2.from = 1;
    ae2.to = 2;
    follower.receive(ae2);

    // conflict at index 1: truncates "old", appends
    // "new" in the same RPC
    REQUIRE(follower.log().size() == 1);
    CHECK(follower.log()[0].value == "new");
}

TEST_CASE("follower updates commitIndex from AE") {
    memory_transport t_f;
    server follower(2, {1, 3}, t_f);

    message ae;
    ae.type = msg_type::append_entries_req;
    ae.term = 1;
    ae.prev_log_index = 0;
    ae.prev_log_term = 0;
    ae.entries = std::vector<log_entry>{
        {1, entry_type::data, "x"}};
    ae.commit_index = 0;
    ae.from = 1;
    ae.to = 2;
    follower.receive(ae);
    CHECK(follower.commit_index() == 0);

    t_f.clear();

    // empty heartbeat with commit=1
    message hb;
    hb.type = msg_type::append_entries_req;
    hb.term = 1;
    hb.prev_log_index = 1;
    hb.prev_log_term = 1;
    hb.entries = std::vector<log_entry>{};
    hb.commit_index = 1;
    hb.from = 1;
    hb.to = 2;
    follower.receive(hb);
    CHECK(follower.commit_index() == 1);
}

TEST_CASE("leader handles successful AE response") {
    memory_transport t;
    auto s = make_leader(t);
    s.client_request("x");

    message resp;
    resp.type = msg_type::append_entries_resp;
    resp.term = s.current_term();
    resp.success = true;
    resp.match_index = 1;
    resp.from = 2;
    resp.to = 1;
    s.receive(resp);

    CHECK(s.next_index_for(2) == 2);
    CHECK(s.match_index_for(2) == 1);
}

TEST_CASE("leader handles failed AE response") {
    memory_transport t;
    auto s = make_leader(t);
    s.client_request("x");

    // simulate nextIndex[2] being too far ahead
    // initial nextIndex is 1 (from become_leader with empty log)
    // after client_request, nextIndex for peers is still 1
    // but let's say follower rejects

    message resp;
    resp.type = msg_type::append_entries_resp;
    resp.term = s.current_term();
    resp.success = false;
    resp.match_index = 0;
    resp.from = 2;
    resp.to = 1;
    s.receive(resp);

    // nextIndex was 1, can't go below 1
    CHECK(s.next_index_for(2) == 1);
}

TEST_CASE("candidate steps down on AE from leader") {
    memory_transport t;
    server s(1, {2, 3}, t);
    s.timeout(); // candidate, term 2

    message ae;
    ae.type = msg_type::append_entries_req;
    ae.term = 2;
    ae.prev_log_index = 0;
    ae.prev_log_term = 0;
    ae.entries = std::vector<log_entry>{};
    ae.commit_index = 0;
    ae.from = 2;
    ae.to = 1;

    s.receive(ae);
    CHECK(s.state() == server_state::follower);
}
