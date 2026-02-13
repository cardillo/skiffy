#include "doctest/doctest.h"
#include "raftpp.h"

using namespace raftpp;

TEST_CASE("become_leader with quorum of votes") {
    memory_transport t;
    server s(1, {2, 3}, t);

    s.timeout(); // candidate, term 2

    // vote from self (via response)
    message v1;
    v1.mtype = msg_type::request_vote_resp;
    v1.mterm = 2;
    v1.mvote_granted = true;
    v1.msource = 2;
    v1.mdest = 1;
    s.receive(v1);

    // one vote (from 2) + self not yet voting for self
    // we need quorum: size > 3/2 => need 2
    CHECK(s.votes_granted().size() == 1);
    s.become_leader(); // should fail, not quorum
    CHECK(s.state() == server_state::candidate);

    message v2;
    v2.mtype = msg_type::request_vote_resp;
    v2.mterm = 2;
    v2.mvote_granted = true;
    v2.msource = 3;
    v2.mdest = 1;
    s.receive(v2);

    CHECK(s.votes_granted().size() == 2);
    s.become_leader();
    CHECK(s.state() == server_state::leader);
}

TEST_CASE("become_leader reinitializes nextIndex/matchIndex") {
    memory_transport t;
    server s(1, {2, 3}, t);

    s.timeout();
    message v;
    v.mtype = msg_type::request_vote_resp;
    v.mterm = 2;
    v.mvote_granted = true;

    v.msource = 2; v.mdest = 1; s.receive(v);
    v.msource = 3; v.mdest = 1; s.receive(v);

    s.become_leader();
    CHECK(s.state() == server_state::leader);

    // nextIndex = len(log) + 1 = 0 + 1 = 1
    CHECK(s.next_index_for(2) == 1);
    CHECK(s.next_index_for(3) == 1);
    CHECK(s.match_index_for(2) == 0);
    CHECK(s.match_index_for(3) == 0);
}

TEST_CASE("become_leader is no-op for follower") {
    memory_transport t;
    server s(1, {2, 3}, t);

    s.become_leader();
    CHECK(s.state() == server_state::follower);
}

TEST_CASE("become_leader is no-op without quorum") {
    memory_transport t;
    server s(1, {2, 3, 4, 5}, t);

    s.timeout(); // candidate

    message v;
    v.mtype = msg_type::request_vote_resp;
    v.mterm = s.current_term();
    v.mvote_granted = true;
    v.msource = 2;
    v.mdest = 1;
    s.receive(v);

    // 1 vote, need 3 for 5-node cluster
    s.become_leader();
    CHECK(s.state() == server_state::candidate);
}
