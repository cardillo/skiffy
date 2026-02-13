#include "doctest/doctest.h"
#include "raftpp.h"

using namespace raftpp;

TEST_CASE("timeout from follower starts election") {
    memory_transport t;
    server s(1, {2, 3}, t);

    CHECK(s.state() == server_state::follower);
    CHECK(s.current_term() == 1);

    s.timeout();

    CHECK(s.state() == server_state::candidate);
    CHECK(s.current_term() == 2);
    CHECK(s.voted_for() == nil_id);
    CHECK(s.votes_responded().empty());
    CHECK(s.votes_granted().empty());
}

TEST_CASE("timeout from candidate restarts election") {
    memory_transport t;
    server s(1, {2, 3}, t);

    s.timeout(); // term 2, candidate
    s.timeout(); // term 3, still candidate

    CHECK(s.state() == server_state::candidate);
    CHECK(s.current_term() == 3);
    CHECK(s.votes_responded().empty());
    CHECK(s.votes_granted().empty());
}

TEST_CASE("timeout is no-op for leader") {
    memory_transport t;
    server s(1, {2, 3}, t);

    s.timeout(); // candidate

    message v;
    v.mtype = msg_type::request_vote_resp;
    v.mterm = s.current_term();
    v.mvote_granted = true;
    v.mdest = 1;

    v.msource = 2; s.receive(v);
    v.msource = 3; s.receive(v);
    s.become_leader();
    CHECK(s.state() == server_state::leader);

    term_t t_before = s.current_term();
    s.timeout(); // should do nothing
    CHECK(s.state() == server_state::leader);
    CHECK(s.current_term() == t_before);
}
