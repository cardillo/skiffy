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
    CHECK(s.voted_for() == 1);   // self-vote
    CHECK(s.votes_responded().empty());
    // self-vote is recorded immediately on timeout
    CHECK(s.votes_granted() ==
          std::set<server_id>{1});
}

TEST_CASE("timeout from candidate restarts election") {
    memory_transport t;
    server s(1, {2, 3}, t);

    s.timeout(); // term 2, candidate
    s.timeout(); // term 3, still candidate

    CHECK(s.state() == server_state::candidate);
    CHECK(s.current_term() == 3);
    CHECK(s.votes_responded().empty());
    // each timeout resets to a fresh self-vote
    CHECK(s.votes_granted() ==
          std::set<server_id>{1});
}

TEST_CASE("timeout is no-op for leader") {
    memory_transport t;
    server s(1, {2, 3}, t);

    s.timeout(); // candidate

    message v;
    v.type = msg_type::request_vote_resp;
    v.term = s.current_term();
    v.vote_granted = true;
    v.to = 1;

    v.from = 2; s.receive(v);
    v.from = 3; s.receive(v);
    s.become_leader();
    CHECK(s.state() == server_state::leader);

    term_t t_before = s.current_term();
    s.timeout(); // should do nothing
    CHECK(s.state() == server_state::leader);
    CHECK(s.current_term() == t_before);
}
