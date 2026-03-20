#include "doctest/doctest.h"

#include "skiffy.hpp"
#include "test_utils.h"

using namespace skiffy;

TEST_CASE("become_leader with quorum of votes") {
    memory_transport t;
    test_server s(s1, {s2, s3}, t);

    s.timeout(); // candidate, term 2

    // timeout() self-votes: {1}; not yet quorum
    // (1 < majority of 3)
    CHECK(s.votes_granted().size() == 1);
    s.become_leader();
    CHECK(s.state() == server_state::candidate);

    // one peer vote: {1,2} = 2/3, majority
    message v1;
    v1.type = msg_type::request_vote_resp;
    v1.term = 2;
    v1.vote_granted = true;
    v1.from = s2;
    v1.to = s1;
    s.receive(v1);

    CHECK(s.votes_granted().size() == 2);
    s.become_leader();
    CHECK(s.state() == server_state::leader);
}

TEST_CASE("become_leader reinitializes nextIndex/matchIndex") {
    memory_transport t;
    test_server s(s1, {s2, s3}, t);

    s.timeout();
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = 2;
    v.vote_granted = true;

    v.from = s2;
    v.to = s1;
    s.receive(v);
    v.from = s3;
    v.to = s1;
    s.receive(v);

    s.become_leader();
    CHECK(s.state() == server_state::leader);

    // nextIndex = len(log) + 1 = 0 + 1 = 1
    CHECK(s.next_index_for(s2) == 1);
    CHECK(s.next_index_for(s3) == 1);
    CHECK(s.match_index_for(s2) == 0);
    CHECK(s.match_index_for(s3) == 0);
}

TEST_CASE("become_leader is no-op for follower") {
    memory_transport t;
    test_server s(s1, {s2, s3}, t);

    s.become_leader();
    CHECK(s.state() == server_state::follower);
}

TEST_CASE("on_become_leader callback fires when leader elected") {
    memory_transport t;
    test_server s(s1, {s2, s3}, t);

    int fired = 0;
    s.on_become_leader([&] { ++fired; });

    s.timeout(); // candidate, term 2

    message v;
    v.type = msg_type::request_vote_resp;
    v.term = 2;
    v.vote_granted = true;
    v.from = s2;
    v.to = s1;
    s.receive(v);

    s.become_leader();
    CHECK(s.state() == server_state::leader);
    CHECK(fired == 1);
}

TEST_CASE("on_become_leader callback does not fire without quorum") {
    memory_transport t;
    test_server s(s1, {s2, s3}, t);

    int fired = 0;
    s.on_become_leader([&] { ++fired; });

    s.timeout();       // candidate, term 2
    s.become_leader(); // no votes yet, stays candidate
    CHECK(s.state() == server_state::candidate);
    CHECK(fired == 0);
}

TEST_CASE("become_leader is no-op without quorum") {
    memory_transport t;
    test_server s(s1, {s2, s3, s4, s5}, t);

    s.timeout(); // candidate

    message v;
    v.type = msg_type::request_vote_resp;
    v.term = s.current_term();
    v.vote_granted = true;
    v.from = s2;
    v.to = s1;
    s.receive(v);

    // 2 votes (self + peer 2), need 3 for 5-node cluster
    s.become_leader();
    CHECK(s.state() == server_state::candidate);
}
