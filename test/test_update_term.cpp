#include "doctest/doctest.h"

#include "raftpp.h"
#include "test_utils.h"

using namespace raftpp;

TEST_CASE("update_term on higher term RPC") {
    memory_transport t;
    server s(1, {2, 3}, t);
    CHECK(s.current_term() == 1);

    // receive any RPC with higher term
    message m;
    m.type = msg_type::request_vote_req;
    m.term = 5;
    m.last_log_term = 0;
    m.last_log_index = 0;
    m.from = 2;
    m.to = 1;

    s.receive(m);

    CHECK(s.current_term() == 5);
    CHECK(s.state() == server_state::follower);
    // voted_for is 2 because after update_term, the
    // request_vote_req is also processed and granted
    CHECK(s.voted_for() == 2);
}

TEST_CASE("leader steps down on higher term") {
    memory_transport t;
    server s(1, {2, 3}, t);

    s.timeout();
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = s.current_term();
    v.vote_granted = true;
    v.to = 1;
    v.from = 2;
    s.receive(v);
    v.from = 3;
    s.receive(v);
    s.become_leader();
    CHECK(s.state() == server_state::leader);

    // receive AE response with higher term
    message m;
    m.type = msg_type::append_entries_resp;
    m.term = 10;
    m.success = false;
    m.match_index = 0;
    m.from = 2;
    m.to = 1;

    s.receive(m);

    CHECK(s.current_term() == 10);
    CHECK(s.state() == server_state::follower);
    CHECK(s.voted_for() == nil_id);
}

TEST_CASE("candidate steps down on higher term") {
    memory_transport t;
    server s(1, {2, 3}, t);
    s.timeout(); // candidate, term 2

    message m;
    m.type = msg_type::request_vote_resp;
    m.term = 5;
    m.vote_granted = false;
    m.from = 2;
    m.to = 1;

    s.receive(m);

    CHECK(s.current_term() == 5);
    CHECK(s.state() == server_state::follower);
}

TEST_CASE("stale vote response is dropped") {
    memory_transport t;
    server s(1, {2, 3}, t);
    s.timeout(); // term 2, candidate

    // stale response from term 1
    message m;
    m.type = msg_type::request_vote_resp;
    m.term = 1;
    m.vote_granted = true;
    m.from = 2;
    m.to = 1;

    s.receive(m);

    CHECK(s.votes_responded().empty());
    // self-vote is present; stale response not recorded
    CHECK(s.votes_granted() == std::set<server_id>{1});
}

TEST_CASE("stale AE response is dropped") {
    memory_transport t;
    server s(1, {2, 3}, t);
    s.timeout();
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = s.current_term();
    v.vote_granted = true;
    v.to = 1;
    v.from = 2;
    s.receive(v);
    v.from = 3;
    s.receive(v);
    s.become_leader();
    t.clear();

    // stale AE response from term 1
    message m;
    m.type = msg_type::append_entries_resp;
    m.term = 1;
    m.success = true;
    m.match_index = 5;
    m.from = 2;
    m.to = 1;

    s.receive(m);

    // should not update matchIndex
    CHECK(s.match_index_for(2) == 0);
}

TEST_CASE("receive ignores message for wrong dest") {
    memory_transport t;
    server s(1, {2, 3}, t);

    message m;
    m.type = msg_type::request_vote_req;
    m.term = 5;
    m.last_log_term = 0;
    m.last_log_index = 0;
    m.from = 2;
    m.to = 3; // not for server 1

    s.receive(m);
    CHECK(s.current_term() == 1); // unchanged
}

TEST_CASE("update_term then handle request_vote_req") {
    memory_transport t;
    server s(1, {2, 3}, t);

    // server at term 1, receives RequestVote at term 3
    message m;
    m.type = msg_type::request_vote_req;
    m.term = 3;
    m.last_log_term = 0;
    m.last_log_index = 0;
    m.from = 2;
    m.to = 1;

    s.receive(m);

    // should have updated to term 3 and granted vote
    CHECK(s.current_term() == 3);
    CHECK(s.voted_for() == 2);
    REQUIRE(t.sent.size() == 1);
    CHECK(t.sent[0].vote_granted.value() == true);
}
