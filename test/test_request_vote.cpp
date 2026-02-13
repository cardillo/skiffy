#include "doctest/doctest.h"
#include "raftpp.h"

using namespace raftpp;

TEST_CASE("candidate sends RequestVote RPC") {
    memory_transport t;
    server s(1, {2, 3}, t);

    s.timeout(); // candidate, term=2
    s.request_vote(2);

    REQUIRE(t.sent.size() == 1);
    auto& m = t.sent[0];
    CHECK(m.mtype == msg_type::request_vote_req);
    CHECK(m.mterm == 2);
    CHECK(m.msource == 1);
    CHECK(m.mdest == 2);
    CHECK(m.mlast_log_term.value() == 0);
    CHECK(m.mlast_log_index.value() == 0);
}

TEST_CASE("request_vote skips already-responded peer") {
    memory_transport t;
    server s(1, {2, 3}, t);

    s.timeout();
    s.request_vote(2);

    // simulate receiving response from 2
    message resp;
    resp.mtype = msg_type::request_vote_resp;
    resp.mterm = s.current_term();
    resp.mvote_granted = false;
    resp.msource = 2;
    resp.mdest = 1;
    s.receive(resp);

    t.clear();
    s.request_vote(2); // should be no-op
    CHECK(t.sent.empty());
}

TEST_CASE("request_vote is no-op for non-candidate") {
    memory_transport t;
    server s(1, {2, 3}, t);

    s.request_vote(2); // follower
    CHECK(t.sent.empty());
}

TEST_CASE("voter grants vote to first requester") {
    memory_transport t_voter;
    server voter(2, {1, 3}, t_voter);

    message req;
    req.mtype = msg_type::request_vote_req;
    req.mterm = 2;
    req.mlast_log_term = 0;
    req.mlast_log_index = 0;
    req.msource = 1;
    req.mdest = 2;

    voter.receive(req);

    REQUIRE(t_voter.sent.size() == 1);
    auto& resp = t_voter.sent[0];
    CHECK(resp.mtype == msg_type::request_vote_resp);
    CHECK(resp.mvote_granted.value() == true);
    CHECK(resp.mdest == 1);
    CHECK(voter.voted_for() == 1);
}

TEST_CASE("voter denies second candidate in same term") {
    memory_transport t_voter;
    server voter(2, {1, 3}, t_voter);

    // first candidate gets the vote
    message req1;
    req1.mtype = msg_type::request_vote_req;
    req1.mterm = 2;
    req1.mlast_log_term = 0;
    req1.mlast_log_index = 0;
    req1.msource = 1;
    req1.mdest = 2;
    voter.receive(req1);

    t_voter.clear();

    // second candidate in same term
    message req2;
    req2.mtype = msg_type::request_vote_req;
    req2.mterm = 2;
    req2.mlast_log_term = 0;
    req2.mlast_log_index = 0;
    req2.msource = 3;
    req2.mdest = 2;
    voter.receive(req2);

    REQUIRE(t_voter.sent.size() == 1);
    CHECK(t_voter.sent[0].mvote_granted.value() == false);
}

TEST_CASE("voter denies candidate with stale log") {
    memory_transport t_voter;
    server voter(2, {1, 3}, t_voter);

    // force voter to leader to add entry, then restart
    voter.timeout(); // term 2 candidate
    message v;
    v.mtype = msg_type::request_vote_resp;
    v.mterm = 2;
    v.mvote_granted = true;
    v.mdest = 2;
    v.msource = 1; voter.receive(v);
    v.msource = 3; voter.receive(v);
    voter.become_leader();
    voter.client_request("x");
    voter.restart();

    // voter has log [{term:2, value:"x"}], term=2
    CHECK(voter.last_term() == 2);

    t_voter.clear();

    // candidate with older term log
    message req;
    req.mtype = msg_type::request_vote_req;
    req.mterm = 3;
    req.mlast_log_term = 1;
    req.mlast_log_index = 1;
    req.msource = 3;
    req.mdest = 2;
    voter.receive(req);

    REQUIRE(t_voter.sent.size() == 1);
    CHECK(t_voter.sent[0].mvote_granted.value() == false);
}

TEST_CASE("vote response tallies votes") {
    memory_transport t;
    server s(1, {2, 3}, t);
    s.timeout(); // candidate, term 2

    message resp;
    resp.mtype = msg_type::request_vote_resp;
    resp.mterm = 2;
    resp.mvote_granted = true;
    resp.msource = 2;
    resp.mdest = 1;
    s.receive(resp);

    CHECK(s.votes_responded().count(2));
    CHECK(s.votes_granted().count(2));

    // denied vote
    message resp2;
    resp2.mtype = msg_type::request_vote_resp;
    resp2.mterm = 2;
    resp2.mvote_granted = false;
    resp2.msource = 3;
    resp2.mdest = 1;
    s.receive(resp2);

    CHECK(s.votes_responded().count(3));
    CHECK_FALSE(s.votes_granted().count(3));
}
