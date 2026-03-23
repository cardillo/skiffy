#include "doctest/doctest.h"

#include "skiffy.hpp"
#include "test_utils.h"

using namespace skiffy;

TEST_CASE("leader sends empty AppendEntries") {
    memory_transport t;
    detail::test_server<memory_transport> s(s1, {s2, s3}, t);
    make_leader(s, t);

    s.append_entries(s2);

    REQUIRE(t.sent.size() == 1);
    auto& m = t.sent[0];
    CHECK(m.type == msg_type::append_entries_req);
    CHECK(m.term == s.current_term());
    CHECK(m.from == s1);
    CHECK(m.to == s2);
    CHECK(m.prev_log_index.value() == 0);
    CHECK(m.prev_log_term.value() == 0);
    CHECK(m.entries.value().empty());
    CHECK(m.commit_index.value() == 0);
}

TEST_CASE("leader sends entry in AppendEntries") {
    memory_transport t;
    detail::test_server<memory_transport> s(s1, {s2, s3}, t);
    make_leader(s, t);
    s.client_request("x");

    s.append_entries(s2);

    REQUIRE(t.sent.size() == 1);
    auto& m = t.sent[0];
    REQUIRE(m.entries.value().size() == 1);
    CHECK(m.entries.value()[0].value == "x");
    CHECK(m.prev_log_index.value() == 0);
}

TEST_CASE("append_entries to self is no-op") {
    memory_transport t;
    detail::test_server<memory_transport> s(s1, {s2, s3}, t);
    make_leader(s, t);
    s.append_entries(s1);
    CHECK(t.sent.empty());
}

TEST_CASE("follower accepts AppendEntries") {
    memory_transport t_follower;
    detail::test_server follower(s2, {s1, s3}, t_follower);

    // append_entries with one entry
    message ae;
    ae.type = msg_type::append_entries_req;
    ae.term = 1;
    ae.prev_log_index = 0;
    ae.prev_log_term = 0;
    ae.entries = std::vector<log_entry>{{1, entry_type::data, "x"}};
    ae.commit_index = 0;
    ae.from = s1;
    ae.to = s2;

    follower.receive(ae);
    CHECK(follower.log().size() == 1);
    CHECK(follower.log()[0].value == "x");
}

TEST_CASE("follower rejects AppendEntries with bad prev") {
    memory_transport t_follower;
    detail::test_server follower(s2, {s1, s3}, t_follower);

    // AE with prevLogIndex=1 but follower has empty log
    message ae;
    ae.type = msg_type::append_entries_req;
    ae.term = 1;
    ae.prev_log_index = 1;
    ae.prev_log_term = 1;
    ae.entries = std::vector<log_entry>{{1, entry_type::data, "x"}};
    ae.commit_index = 0;
    ae.from = s1;
    ae.to = s2;

    follower.receive(ae);

    REQUIRE(t_follower.sent.size() == 1);
    CHECK(t_follower.sent[0].success.value() == false);
    CHECK(follower.log().empty());
}

TEST_CASE("follower truncates conflicting entries") {
    memory_transport t_f;
    detail::test_server follower(s2, {s1, s3}, t_f);

    // give follower a log entry at term 1
    message ae1;
    ae1.type = msg_type::append_entries_req;
    ae1.term = 1;
    ae1.prev_log_index = 0;
    ae1.prev_log_term = 0;
    ae1.entries = std::vector<log_entry>{{1, entry_type::data, "old"}};
    ae1.commit_index = 0;
    ae1.from = s1;
    ae1.to = s2;
    follower.receive(ae1);
    CHECK(follower.log().size() == 1);

    t_f.clear();

    // new leader sends entry at index 1 with different term
    message ae2;
    ae2.type = msg_type::append_entries_req;
    ae2.term = 2;
    ae2.prev_log_index = 0;
    ae2.prev_log_term = 0;
    ae2.entries = std::vector<log_entry>{{2, entry_type::data, "new"}};
    ae2.commit_index = 0;
    ae2.from = s1;
    ae2.to = s2;
    follower.receive(ae2);

    // conflict at index 1: truncates "old", appends
    // "new" in the same RPC
    REQUIRE(follower.log().size() == 1);
    CHECK(follower.log()[0].value == "new");
}

TEST_CASE("follower updates commitIndex from AE") {
    memory_transport t_f;
    detail::test_server follower(s2, {s1, s3}, t_f);

    message ae;
    ae.type = msg_type::append_entries_req;
    ae.term = 1;
    ae.prev_log_index = 0;
    ae.prev_log_term = 0;
    ae.entries = std::vector<log_entry>{{1, entry_type::data, "x"}};
    ae.commit_index = 0;
    ae.from = s1;
    ae.to = s2;
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
    hb.from = s1;
    hb.to = s2;
    follower.receive(hb);
    CHECK(follower.commit_index() == 1);
}

TEST_CASE("leader handles successful AE response") {
    memory_transport t;
    detail::test_server<memory_transport> s(s1, {s2, s3}, t);
    make_leader(s, t);
    s.client_request("x");

    message resp;
    resp.type = msg_type::append_entries_resp;
    resp.term = s.current_term();
    resp.success = true;
    resp.match_index = 1;
    resp.from = s2;
    resp.to = s1;
    s.receive(resp);

    CHECK(s.next_index_for(s2) == 2);
    CHECK(s.match_index_for(s2) == 1);
}

TEST_CASE("leader handles failed AE response") {
    memory_transport t;
    detail::test_server<memory_transport> s(s1, {s2, s3}, t);
    make_leader(s, t);
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
    resp.from = s2;
    resp.to = s1;
    s.receive(resp);

    // nextIndex was 1, can't go below 1
    CHECK(s.next_index_for(s2) == 1);
}

TEST_CASE("candidate steps down on AE from leader") {
    memory_transport t;
    detail::test_server s(s1, {s2, s3}, t);
    s.timeout(); // candidate, term 2

    message ae;
    ae.type = msg_type::append_entries_req;
    ae.term = 2;
    ae.prev_log_index = 0;
    ae.prev_log_term = 0;
    ae.entries = std::vector<log_entry>{};
    ae.commit_index = 0;
    ae.from = s2;
    ae.to = s1;

    s.receive(ae);
    CHECK(s.state() == detail::server_state::follower);
}

TEST_CASE("on_entries_dropped fires on log conflict") {
    memory_transport t_f;
    detail::test_server follower(s2, {s1, s3}, t_f);

    // give follower an entry at term 1
    message ae1;
    ae1.type = msg_type::append_entries_req;
    ae1.term = 1;
    ae1.prev_log_index = 0;
    ae1.prev_log_term = 0;
    ae1.entries = std::vector<log_entry>{{1, entry_type::data, "old"}};
    ae1.commit_index = 0;
    ae1.from = s1;
    ae1.to = s2;
    follower.receive(ae1);
    REQUIRE(follower.log().size() == 1);

    std::vector<log_entry> dropped;
    follower.on_entries_dropped(
        [&](std::vector<log_entry> d) { dropped = std::move(d); });

    // new leader sends conflicting entry at same index with higher term
    message ae2;
    ae2.type = msg_type::append_entries_req;
    ae2.term = 2;
    ae2.prev_log_index = 0;
    ae2.prev_log_term = 0;
    ae2.entries = std::vector<log_entry>{{2, entry_type::data, "new"}};
    ae2.commit_index = 0;
    ae2.from = s1;
    ae2.to = s2;
    follower.receive(ae2);

    REQUIRE(dropped.size() == 1);
    CHECK(dropped[0].value == "old");
    CHECK(follower.log()[0].value == "new");
}

TEST_CASE("client_request returns nullopt on follower") {
    memory_transport t;
    detail::test_server follower(s2, {s1, s3}, t);
    CHECK(!follower.client_request("x").has_value());
}

TEST_CASE("client_request returns log index on leader") {
    memory_transport t;
    detail::test_server<memory_transport> s(s1, {s2, s3}, t);
    make_leader(s, t);
    CHECK(s.client_request("x") == index_t{1});
    CHECK(s.client_request("y") == index_t{2});
}
