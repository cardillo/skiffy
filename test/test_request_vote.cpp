#include "doctest/doctest.h"

#include "raftpp.h"
#include "test_utils.h"

using namespace raftpp;

TEST_CASE("candidate sends RequestVote RPC") {
    memory_transport t;
    server s(s1, {s2, s3}, t);

    s.timeout(); // candidate, term=2
    s.request_vote(s2);

    REQUIRE(t.sent.size() == 1);
    auto& m = t.sent[0];
    CHECK(m.type == msg_type::request_vote_req);
    CHECK(m.term == 2);
    CHECK(m.from == s1);
    CHECK(m.to == s2);
    CHECK(m.last_log_term.value() == 0);
    CHECK(m.last_log_index.value() == 0);
}

TEST_CASE("request_vote skips already-responded peer") {
    memory_transport t;
    server s(s1, {s2, s3}, t);

    s.timeout();
    s.request_vote(s2);

    // simulate receiving response from 2
    message resp;
    resp.type = msg_type::request_vote_resp;
    resp.term = s.current_term();
    resp.vote_granted = false;
    resp.from = s2;
    resp.to = s1;
    s.receive(resp);

    t.clear();
    s.request_vote(s2); // should be no-op
    CHECK(t.sent.empty());
}

TEST_CASE("request_vote is no-op for non-candidate") {
    memory_transport t;
    server s(s1, {s2, s3}, t);

    s.request_vote(s2); // follower
    CHECK(t.sent.empty());
}

TEST_CASE("voter grants vote to first requester") {
    memory_transport t_voter;
    server voter(s2, {s1, s3}, t_voter);

    message req;
    req.type = msg_type::request_vote_req;
    req.term = 2;
    req.last_log_term = 0;
    req.last_log_index = 0;
    req.from = s1;
    req.to = s2;

    voter.receive(req);

    REQUIRE(t_voter.sent.size() == 1);
    auto& resp = t_voter.sent[0];
    CHECK(resp.type == msg_type::request_vote_resp);
    CHECK(resp.vote_granted.value() == true);
    CHECK(resp.to == s1);
    CHECK(voter.voted_for() == s1);
}

TEST_CASE("voter denies second candidate in same term") {
    memory_transport t_voter;
    server voter(s2, {s1, s3}, t_voter);

    // first candidate gets the vote
    message req1;
    req1.type = msg_type::request_vote_req;
    req1.term = 2;
    req1.last_log_term = 0;
    req1.last_log_index = 0;
    req1.from = s1;
    req1.to = s2;
    voter.receive(req1);

    t_voter.clear();

    // second candidate in same term
    message req2;
    req2.type = msg_type::request_vote_req;
    req2.term = 2;
    req2.last_log_term = 0;
    req2.last_log_index = 0;
    req2.from = s3;
    req2.to = s2;
    voter.receive(req2);

    REQUIRE(t_voter.sent.size() == 1);
    CHECK(t_voter.sent[0].vote_granted.value() == false);
}

TEST_CASE("voter denies candidate with stale log") {
    memory_transport t_voter;
    server voter(s2, {s1, s3}, t_voter);

    // force voter to leader to add entry, then restart
    voter.timeout(); // term 2 candidate
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = 2;
    v.vote_granted = true;
    v.to = s2;
    v.from = s1;
    voter.receive(v);
    v.from = s3;
    voter.receive(v);
    voter.become_leader();
    voter.client_request("x");
    voter.restart();

    // voter has log [{term:2, value:"x"}], term=2
    CHECK(voter.last_term() == 2);

    t_voter.clear();

    // candidate with older term log
    message req;
    req.type = msg_type::request_vote_req;
    req.term = 3;
    req.last_log_term = 1;
    req.last_log_index = 1;
    req.from = s3;
    req.to = s2;
    voter.receive(req);

    REQUIRE(t_voter.sent.size() == 1);
    CHECK(t_voter.sent[0].vote_granted.value() == false);
}

TEST_CASE("vote response tallies votes") {
    memory_transport t;
    server s(s1, {s2, s3}, t);
    s.timeout(); // candidate, term 2

    message resp;
    resp.type = msg_type::request_vote_resp;
    resp.term = 2;
    resp.vote_granted = true;
    resp.from = s2;
    resp.to = s1;
    s.receive(resp);

    CHECK(s.votes_responded().count(s2));
    CHECK(s.votes_granted().count(s2));

    // denied vote
    message resp2;
    resp2.type = msg_type::request_vote_resp;
    resp2.term = 2;
    resp2.vote_granted = false;
    resp2.from = s3;
    resp2.to = s1;
    s.receive(resp2);

    CHECK(s.votes_responded().count(s3));
    CHECK_FALSE(s.votes_granted().count(s3));
}
