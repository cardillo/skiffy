#include "doctest/doctest.h"
#include "raftpp.h"

using namespace raftpp;

static server<memory_transport>
make_leader(memory_transport& t) {
    server<memory_transport> s(1, {2, 3}, t);
    s.timeout();
    message v;
    v.mtype = msg_type::request_vote_resp;
    v.mterm = s.current_term();
    v.mvote_granted = true;
    v.mdest = 1;
    v.msource = 2; s.receive(v);
    v.msource = 3; s.receive(v);
    s.become_leader();
    t.clear();
    return s;
}

TEST_CASE("leader sends empty AppendEntries") {
    memory_transport t;
    auto s = make_leader(t);

    s.append_entries(2);

    REQUIRE(t.sent.size() == 1);
    auto& m = t.sent[0];
    CHECK(m.mtype == msg_type::append_entries_req);
    CHECK(m.mterm == s.current_term());
    CHECK(m.msource == 1);
    CHECK(m.mdest == 2);
    CHECK(m.mprev_log_index.value() == 0);
    CHECK(m.mprev_log_term.value() == 0);
    CHECK(m.mentries.value().empty());
    CHECK(m.mcommit_index.value() == 0);
}

TEST_CASE("leader sends entry in AppendEntries") {
    memory_transport t;
    auto s = make_leader(t);
    s.client_request("x");

    s.append_entries(2);

    REQUIRE(t.sent.size() == 1);
    auto& m = t.sent[0];
    REQUIRE(m.mentries.value().size() == 1);
    CHECK(m.mentries.value()[0].value == "x");
    CHECK(m.mprev_log_index.value() == 0);
}

TEST_CASE("append_entries to self is no-op") {
    memory_transport t;
    auto s = make_leader(t);
    s.append_entries(1);
    CHECK(t.sent.empty());
}

TEST_CASE("follower accepts AppendEntries") {
    memory_transport t_follower;
    server follower(2, {1, 3}, t_follower);

    // append_entries with one entry
    message ae;
    ae.mtype = msg_type::append_entries_req;
    ae.mterm = 1;
    ae.mprev_log_index = 0;
    ae.mprev_log_term = 0;
    ae.mentries = std::vector<log_entry>{{1, "x"}};
    ae.mcommit_index = 0;
    ae.msource = 1;
    ae.mdest = 2;

    follower.receive(ae);
    CHECK(follower.log().size() == 1);
    CHECK(follower.log()[0].value == "x");
}

TEST_CASE("follower rejects AppendEntries with bad prev") {
    memory_transport t_follower;
    server follower(2, {1, 3}, t_follower);

    // AE with prevLogIndex=1 but follower has empty log
    message ae;
    ae.mtype = msg_type::append_entries_req;
    ae.mterm = 1;
    ae.mprev_log_index = 1;
    ae.mprev_log_term = 1;
    ae.mentries = std::vector<log_entry>{{1, "x"}};
    ae.mcommit_index = 0;
    ae.msource = 1;
    ae.mdest = 2;

    follower.receive(ae);

    REQUIRE(t_follower.sent.size() == 1);
    CHECK(t_follower.sent[0].msuccess.value() == false);
    CHECK(follower.log().empty());
}

TEST_CASE("follower truncates conflicting entries") {
    memory_transport t_f;
    server follower(2, {1, 3}, t_f);

    // give follower a log entry at term 1
    message ae1;
    ae1.mtype = msg_type::append_entries_req;
    ae1.mterm = 1;
    ae1.mprev_log_index = 0;
    ae1.mprev_log_term = 0;
    ae1.mentries = std::vector<log_entry>{{1, "old"}};
    ae1.mcommit_index = 0;
    ae1.msource = 1;
    ae1.mdest = 2;
    follower.receive(ae1);
    CHECK(follower.log().size() == 1);

    t_f.clear();

    // new leader sends entry at index 1 with different term
    message ae2;
    ae2.mtype = msg_type::append_entries_req;
    ae2.mterm = 2;
    ae2.mprev_log_index = 0;
    ae2.mprev_log_term = 0;
    ae2.mentries = std::vector<log_entry>{{2, "new"}};
    ae2.mcommit_index = 0;
    ae2.msource = 1;
    ae2.mdest = 2;
    follower.receive(ae2);

    // conflict detected: truncate, then on next receive, append
    // after truncation, log should be empty
    CHECK(follower.log().empty());

    t_f.clear();
    // resend same AE, now log is empty so it appends
    follower.receive(ae2);
    REQUIRE(follower.log().size() == 1);
    CHECK(follower.log()[0].value == "new");
}

TEST_CASE("follower updates commitIndex from AE") {
    memory_transport t_f;
    server follower(2, {1, 3}, t_f);

    message ae;
    ae.mtype = msg_type::append_entries_req;
    ae.mterm = 1;
    ae.mprev_log_index = 0;
    ae.mprev_log_term = 0;
    ae.mentries = std::vector<log_entry>{{1, "x"}};
    ae.mcommit_index = 0;
    ae.msource = 1;
    ae.mdest = 2;
    follower.receive(ae);
    CHECK(follower.commit_index() == 0);

    t_f.clear();

    // empty heartbeat with commit=1
    message hb;
    hb.mtype = msg_type::append_entries_req;
    hb.mterm = 1;
    hb.mprev_log_index = 1;
    hb.mprev_log_term = 1;
    hb.mentries = std::vector<log_entry>{};
    hb.mcommit_index = 1;
    hb.msource = 1;
    hb.mdest = 2;
    follower.receive(hb);
    CHECK(follower.commit_index() == 1);
}

TEST_CASE("leader handles successful AE response") {
    memory_transport t;
    auto s = make_leader(t);
    s.client_request("x");

    message resp;
    resp.mtype = msg_type::append_entries_resp;
    resp.mterm = s.current_term();
    resp.msuccess = true;
    resp.mmatch_index = 1;
    resp.msource = 2;
    resp.mdest = 1;
    s.receive(resp);

    CHECK(s.next_index_for(2) == 2);
    CHECK(s.match_index_for(2) == 1);
}

TEST_CASE("leader handles failed AE response") {
    memory_transport t;
    auto s = make_leader(t);
    s.client_request("x");

    // simulate nextIndex[2] being too far ahead
    // initial nextIndex is 1 (from become_leader with empty log)
    // after client_request, nextIndex for peers is still 1
    // but let's say follower rejects

    message resp;
    resp.mtype = msg_type::append_entries_resp;
    resp.mterm = s.current_term();
    resp.msuccess = false;
    resp.mmatch_index = 0;
    resp.msource = 2;
    resp.mdest = 1;
    s.receive(resp);

    // nextIndex was 1, can't go below 1
    CHECK(s.next_index_for(2) == 1);
}

TEST_CASE("candidate steps down on AE from leader") {
    memory_transport t;
    server s(1, {2, 3}, t);
    s.timeout(); // candidate, term 2

    message ae;
    ae.mtype = msg_type::append_entries_req;
    ae.mterm = 2;
    ae.mprev_log_index = 0;
    ae.mprev_log_term = 0;
    ae.mentries = std::vector<log_entry>{};
    ae.mcommit_index = 0;
    ae.msource = 2;
    ae.mdest = 1;

    s.receive(ae);
    CHECK(s.state() == server_state::follower);
}
