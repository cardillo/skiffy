#include "doctest/doctest.h"

#include "raftpp.h"
#include "test_utils.h"

using namespace raftpp;

static server<memory_transport> make_leader(memory_transport& t) {
    server<memory_transport> s(1, {2, 3}, t);
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
    return s;
}

TEST_CASE("advance_commit_index with quorum match") {
    memory_transport t;
    auto s = make_leader(t);
    s.client_request("x");

    // simulate follower 2 replicated entry 1
    message resp;
    resp.type = msg_type::append_entries_resp;
    resp.term = s.current_term();
    resp.success = true;
    resp.match_index = 1;
    resp.from = 2;
    resp.to = 1;
    s.receive(resp);

    CHECK(s.match_index_for(2) == 1);

    s.advance_commit_index();
    // leader (self) + server 2 = quorum of 2 out of 3
    CHECK(s.commit_index() == 1);
}

TEST_CASE("advance_commit_index needs current term") {
    memory_transport t;
    auto s = make_leader(t);

    // manually insert a log entry from a prior term
    // we can't do this through the public API normally,
    // so we'll use client_request (which uses current term)
    s.client_request("x");

    // now simulate stepping down and re-election at higher term
    // (restart, timeout, get votes, become leader again)
    s.restart();
    s.timeout(); // term 3 candidate
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

    // entry at index 1 has term 2, but current term is 3
    CHECK(s.log()[0].term == 2);
    CHECK(s.current_term() == 3);

    // even if all agree on index 1, can't commit
    // because entry term != current term
    message resp;
    resp.type = msg_type::append_entries_resp;
    resp.term = 3;
    resp.success = true;
    resp.match_index = 1;
    resp.from = 2;
    resp.to = 1;
    s.receive(resp);

    s.advance_commit_index();
    CHECK(s.commit_index() == 0);

    // add entry in current term, then commit both
    s.client_request("y");
    CHECK(s.log()[1].term == 3);

    resp.match_index = 2;
    s.receive(resp);
    s.advance_commit_index();
    CHECK(s.commit_index() == 2);
}

TEST_CASE("advance_commit_index is no-op for follower") {
    memory_transport t;
    server s(1, {2, 3}, t);
    s.advance_commit_index();
    CHECK(s.commit_index() == 0);
}

TEST_CASE("single-server cluster commits immediately") {
    memory_transport t;
    server s(1, {}, t);

    s.timeout();
    // candidate must vote for itself via RequestVote(i,i)
    s.request_vote(1);
    REQUIRE(t.sent.size() == 1);
    s.receive(t.sent[0]); // processes req, emits resp
    REQUIRE(t.sent.size() == 2);
    s.receive(t.sent[1]); // processes resp, tallies vote
    t.clear();

    s.become_leader();
    CHECK(s.state() == server_state::leader);

    s.client_request("x");
    s.advance_commit_index();
    CHECK(s.commit_index() == 1);
}
