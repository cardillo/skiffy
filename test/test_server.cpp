#include "doctest/doctest.h"

#include "skiffy.h"
#include "test_utils.h"

using namespace skiffy;

static void grant_vote(server<memory_transport>& s, server_id src) {
    message rv;
    rv.type = msg_type::request_vote_resp;
    rv.term = s.current_term();
    rv.from = src;
    rv.to = s.id();
    rv.vote_granted = true;
    s.receive(rv);
}

// -------------------------------------------------------
// election tests
// -------------------------------------------------------

TEST_CASE("single-node election succeeds") {
    memory_transport t;
    server<memory_transport> s(s1, {}, t);

    s.timeout();
    CHECK(s.state() == server_state::candidate);
    // self-vote means quorum is already satisfied
    s.become_leader();
    CHECK(s.state() == server_state::leader);
}

TEST_CASE("3-node cluster elects with one peer vote") {
    memory_transport t;
    server<memory_transport> s(s1, {s2, s3}, t);

    s.timeout();
    // self-vote + one peer vote = 2/3 = majority
    grant_vote(s, s2);
    s.become_leader();
    CHECK(s.state() == server_state::leader);
}

TEST_CASE("candidate needs majority, not all peers") {
    // 5-node cluster: need 3 votes (self + 2 peers)
    memory_transport t;
    server<memory_transport> s(s1, {s2, s3, s4, s5}, t);

    s.timeout();
    grant_vote(s, s2);
    // 2 votes so far (self + peer 2): not majority of 5
    s.become_leader();
    CHECK(s.state() == server_state::candidate);

    grant_vote(s, s3);
    // 3 votes (self + 2,3): majority of 5
    s.become_leader();
    CHECK(s.state() == server_state::leader);
}

TEST_CASE("timeout increments term and self-votes") {
    memory_transport t;
    server<memory_transport> s(s1, {s2}, t);

    s.timeout();
    CHECK(s.current_term() == 2);
    CHECK(s.voted_for() == s1);
    CHECK(s.votes_granted().count(s1) == 1);
}

TEST_CASE("higher-term message demotes to follower") {
    memory_transport t;
    server<memory_transport> s(s1, {s2, s3}, t);

    s.timeout();
    grant_vote(s, s2);
    grant_vote(s, s3);
    s.become_leader();
    REQUIRE(s.state() == server_state::leader);

    message m;
    m.type = msg_type::request_vote_req;
    m.term = s.current_term() + 1;
    m.from = s2;
    m.to = s1;
    m.last_log_term = 0;
    m.last_log_index = 0;
    s.receive(m);

    CHECK(s.state() == server_state::follower);
    CHECK(s.current_term() == 3);
}

TEST_CASE("stale vote response is dropped") {
    memory_transport t;
    server<memory_transport> s(s1, {s2, s3}, t);

    s.timeout(); // term=2
    term_t old_term = s.current_term();

    s.timeout(); // term=3, new election

    // deliver a response for the old term
    message rv;
    rv.type = msg_type::request_vote_resp;
    rv.term = old_term;
    rv.from = s2;
    rv.to = s1;
    rv.vote_granted = true;
    s.receive(rv);

    // only self-vote from the current election counted
    CHECK(s.votes_granted().count(s2) == 0);
    CHECK(s.votes_granted().size() == 1); // just self
}

// -------------------------------------------------------
// restart tests
// -------------------------------------------------------

TEST_CASE("restart preserves log and term") {
    memory_transport t;
    server<memory_transport> s(s1, {s2, s3}, t);

    s.timeout();
    grant_vote(s, s2);
    grant_vote(s, s3);
    s.become_leader();
    s.client_request("hello");

    term_t saved_term = s.current_term();
    size_t saved_log = s.log().size();

    s.restart();

    CHECK(s.state() == server_state::follower);
    CHECK(s.current_term() == saved_term);
    CHECK(s.log().size() == saved_log);
    CHECK(s.commit_index() == 0);
    CHECK(s.votes_granted().empty());
}

// -------------------------------------------------------
// replication and commit tests
// -------------------------------------------------------

TEST_CASE("advance_commit_index commits majority") {
    memory_transport t;
    server<memory_transport> leader(s1, {s2, s3}, t);
    server<memory_transport> f2(s2, {s1, s3}, t);
    server<memory_transport> f3(s3, {s1, s2}, t);

    // elect leader
    leader.timeout();
    leader.request_vote(s2);
    leader.request_vote(s3);

    // deliver vote requests to followers
    t.deliver([&](const message& m) {
        if (m.to == s2)
            f2.receive(m);
        if (m.to == s3)
            f3.receive(m);
    });

    // deliver vote responses to leader
    t.deliver([&](const message& m) { leader.receive(m); });

    leader.become_leader();
    REQUIRE(leader.state() == server_state::leader);

    leader.client_request("x");
    REQUIRE(leader.log().size() == 1);

    // replicate to both followers
    leader.append_entries(s2);
    leader.append_entries(s3);

    t.deliver([&](const message& m) {
        if (m.to == s2)
            f2.receive(m);
        if (m.to == s3)
            f3.receive(m);
    });

    // deliver AE responses to leader
    t.deliver([&](const message& m) { leader.receive(m); });

    leader.advance_commit_index();
    CHECK(leader.commit_index() == 1);
}

TEST_CASE("follower overwrites conflicting entries") {
    memory_transport t;
    server<memory_transport> leader(s1, {s2}, t);
    server<memory_transport> follower(s2, {s1}, t);

    // plant a stale entry (term=1) on the follower
    {
        message ae;
        ae.type = msg_type::append_entries_req;
        ae.term = 1;
        ae.from = server_id(s5); // old leader
        ae.to = s2;
        ae.prev_log_index = 0;
        ae.prev_log_term = 0;
        ae.entries = {log_entry{1, entry_type::data, "old"}};
        ae.commit_index = 0;
        follower.receive(ae);
    }
    t.clear();
    REQUIRE(follower.log().size() == 1);
    REQUIRE(follower.log()[0].value == "old");

    // elect leader at term=2
    leader.timeout();
    grant_vote(leader, s2);
    leader.become_leader();
    REQUIRE(leader.state() == server_state::leader);

    leader.client_request("new");
    t.clear();
    leader.append_entries(s2);

    // follower: conflict at index 1, truncates "old",
    // appends "new"
    t.deliver([&](const message& m) {
        if (m.to == s2)
            follower.receive(m);
    });

    REQUIRE(follower.log().size() == 1);
    CHECK(follower.log()[0].value == "new");
    CHECK(follower.log()[0].term == 2);
}

TEST_CASE("leader replicates batch of entries") {
    memory_transport t;
    server<memory_transport> leader(s1, {s2}, t);
    server<memory_transport> follower(s2, {s1}, t);

    leader.timeout();
    grant_vote(leader, s2);
    leader.become_leader();
    t.clear();

    leader.client_request("a");
    leader.client_request("b");
    leader.client_request("c");
    leader.append_entries(s2);

    t.deliver([&](const message& m) {
        if (m.to == s2)
            follower.receive(m);
    });

    REQUIRE(follower.log().size() == 3);
    CHECK(follower.log()[0].value == "a");
    CHECK(follower.log()[1].value == "b");
    CHECK(follower.log()[2].value == "c");
}
