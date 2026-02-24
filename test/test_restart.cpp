#include "doctest/doctest.h"
#include "raftpp.h"

using namespace raftpp;

TEST_CASE("restart resets volatile state") {
    memory_transport t;
    server s(1, {2, 3}, t);

    // mutate state away from init
    s.timeout(); // becomes candidate, term=2
    CHECK(s.state() == server_state::candidate);
    CHECK(s.current_term() == 2);

    s.restart();

    CHECK(s.state() == server_state::follower);
    CHECK(s.commit_index() == 0);
    CHECK(s.votes_responded().empty());
    CHECK(s.votes_granted().empty());
    CHECK(s.next_index_for(2) == 1);
    CHECK(s.match_index_for(2) == 0);
}

TEST_CASE("restart preserves durable state") {
    memory_transport t;
    server s(1, {2, 3}, t);

    s.timeout(); // term becomes 2
    term_t term_before = s.current_term();
    server_id voted_before = s.voted_for();

    s.restart();

    CHECK(s.current_term() == term_before);
    CHECK(s.voted_for() == voted_before);
    CHECK(s.log().empty()); // log preserved (was empty)
}

TEST_CASE("restart preserves log") {
    memory_transport t;
    server s(1, {2, 3}, t);

    // force to leader so we can add entries
    s.timeout();
    s.votes_granted().size(); // just candidate
    // manually receive votes to become leader
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = s.current_term();
    v.vote_granted = true;
    v.from = 2;
    v.to = 1;
    s.receive(v);

    v.from = 3;
    s.receive(v);

    s.become_leader();
    CHECK(s.state() == server_state::leader);

    s.client_request("hello");
    CHECK(s.log().size() == 1);

    s.restart();
    CHECK(s.log().size() == 1);
    CHECK(s.log()[0].value == "hello");
}
