#include "doctest/doctest.h"

#include "skiffy.h"
#include "test_utils.h"

using skiffy::server_id;

TEST_CASE("timeout from follower starts election") {
    skiffy::memory_transport t;
    skiffy::server s(s1, {s2, s3}, t);

    CHECK(s.state() == skiffy::server_state::follower);
    CHECK(s.current_term() == 1);

    s.timeout();

    CHECK(s.state() == skiffy::server_state::candidate);
    CHECK(s.current_term() == 2);
    CHECK(s.voted_for() == s1); // self-vote
    CHECK(s.votes_responded().empty());
    // self-vote is recorded immediately on timeout
    CHECK(s.votes_granted() == std::set<skiffy::server_id>{s1});
}

TEST_CASE("timeout from candidate restarts election") {
    skiffy::memory_transport t;
    skiffy::server s(s1, {s2, s3}, t);

    s.timeout(); // term 2, candidate
    s.timeout(); // term 3, still candidate

    CHECK(s.state() == skiffy::server_state::candidate);
    CHECK(s.current_term() == 3);
    CHECK(s.votes_responded().empty());
    // each timeout resets to a fresh self-vote
    CHECK(s.votes_granted() == std::set<skiffy::server_id>{s1});
}

TEST_CASE("timeout is no-op for leader") {
    skiffy::memory_transport t;
    skiffy::server s(s1, {s2, s3}, t);

    s.timeout(); // candidate

    skiffy::message v;
    v.type = skiffy::msg_type::request_vote_resp;
    v.term = s.current_term();
    v.vote_granted = true;
    v.to = s1;

    v.from = s2;
    s.receive(v);
    v.from = s3;
    s.receive(v);
    s.become_leader();
    CHECK(s.state() == skiffy::server_state::leader);

    skiffy::term_t t_before = s.current_term();
    s.timeout(); // should do nothing
    CHECK(s.state() == skiffy::server_state::leader);
    CHECK(s.current_term() == t_before);
}
