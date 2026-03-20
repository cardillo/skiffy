#include "doctest/doctest.h"

#include "skiffy.hpp"
#include "test_utils.h"

using namespace skiffy;

TEST_CASE("update_term on higher term RPC") {
    memory_transport t;
    test_server s(s1, {s2, s3}, t);
    CHECK(s.current_term() == 1);

    // receive any RPC with higher term
    message m;
    m.type = msg_type::request_vote_req;
    m.term = 5;
    m.last_log_term = 0;
    m.last_log_index = 0;
    m.from = s2;
    m.to = s1;

    s.receive(m);

    CHECK(s.current_term() == 5);
    CHECK(s.state() == server_state::follower);
    // voted_for is 2 because after update_term, the
    // request_vote_req is also processed and granted
    CHECK(s.voted_for() == s2);
}

TEST_CASE("leader steps down on higher term") {
    memory_transport t;
    test_server s(s1, {s2, s3}, t);

    s.timeout();
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = s.current_term();
    v.vote_granted = true;
    v.to = s1;
    v.from = s2;
    s.receive(v);
    v.from = s3;
    s.receive(v);
    s.become_leader();
    CHECK(s.state() == server_state::leader);

    // receive AE response with higher term
    message m;
    m.type = msg_type::append_entries_resp;
    m.term = 10;
    m.success = false;
    m.match_index = 0;
    m.from = s2;
    m.to = s1;

    s.receive(m);

    CHECK(s.current_term() == 10);
    CHECK(s.state() == server_state::follower);
    CHECK(s.voted_for() == nil_id);
}

TEST_CASE("candidate steps down on higher term") {
    memory_transport t;
    test_server s(s1, {s2, s3}, t);
    s.timeout(); // candidate, term 2

    message m;
    m.type = msg_type::request_vote_resp;
    m.term = 5;
    m.vote_granted = false;
    m.from = s2;
    m.to = s1;

    s.receive(m);

    CHECK(s.current_term() == 5);
    CHECK(s.state() == server_state::follower);
}

TEST_CASE("stale vote response is dropped") {
    memory_transport t;
    test_server s(s1, {s2, s3}, t);
    s.timeout(); // term 2, candidate

    // stale response from term 1
    message m;
    m.type = msg_type::request_vote_resp;
    m.term = 1;
    m.vote_granted = true;
    m.from = s2;
    m.to = s1;

    s.receive(m);

    CHECK(s.votes_responded().empty());
    // self-vote is present; stale response not recorded
    CHECK(s.votes_granted() == std::set<node_id>{s1});
}

TEST_CASE("stale AE response is dropped") {
    memory_transport t;
    test_server s(s1, {s2, s3}, t);
    s.timeout();
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = s.current_term();
    v.vote_granted = true;
    v.to = s1;
    v.from = s2;
    s.receive(v);
    v.from = s3;
    s.receive(v);
    s.become_leader();
    t.clear();

    // stale AE response from term 1
    message m;
    m.type = msg_type::append_entries_resp;
    m.term = 1;
    m.success = true;
    m.match_index = 5;
    m.from = s2;
    m.to = s1;

    s.receive(m);

    // should not update matchIndex
    CHECK(s.match_index_for(s2) == 0);
}

TEST_CASE("receive ignores message for wrong dest") {
    memory_transport t;
    test_server s(s1, {s2, s3}, t);

    message m;
    m.type = msg_type::request_vote_req;
    m.term = 5;
    m.last_log_term = 0;
    m.last_log_index = 0;
    m.from = s2;
    m.to = s3; // not for server 1

    s.receive(m);
    CHECK(s.current_term() == 1); // unchanged
}

TEST_CASE("update_term then handle request_vote_req") {
    memory_transport t;
    test_server s(s1, {s2, s3}, t);

    // server at term 1, receives RequestVote at term 3
    message m;
    m.type = msg_type::request_vote_req;
    m.term = 3;
    m.last_log_term = 0;
    m.last_log_index = 0;
    m.from = s2;
    m.to = s1;

    s.receive(m);

    // should have updated to term 3 and granted vote
    CHECK(s.current_term() == 3);
    CHECK(s.voted_for() == s2);
    REQUIRE(t.sent.size() == 1);
    CHECK(t.sent[0].vote_granted.value() == true);
}
