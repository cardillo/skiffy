#include "doctest/doctest.h"

#include "raftpp.h"
#include "test_utils.h"

using namespace raftpp;

// -------------------------------------------------------
// helpers
// -------------------------------------------------------

// replicate entry to both followers and commit
static void replicate_and_commit(server<memory_transport>& leader,
                                 server<memory_transport>& f2,
                                 server<memory_transport>& f3,
                                 memory_transport& t) {
    leader.append_entries(2);
    leader.append_entries(3);
    deliver(t, [&](const message& m) {
        if (m.to == 2)
            f2.receive(m);
        if (m.to == 3)
            f3.receive(m);
    });
    deliver(t, [&](const message& m) { leader.receive(m); });
    leader.advance_commit_index();
    // propagate commit via heartbeat
    leader.append_entries(2);
    leader.append_entries(3);
    deliver(t, [&](const message& m) {
        if (m.to == 2)
            f2.receive(m);
        if (m.to == 3)
            f3.receive(m);
    });
    t.clear();
}

// -------------------------------------------------------
// compact() tests
// -------------------------------------------------------

TEST_CASE("compact discards entries up to commit") {
    memory_transport t;
    server<memory_transport> leader(1, {2, 3}, t);
    server<memory_transport> f2(2, {1, 3}, t);
    server<memory_transport> f3(3, {1, 2}, t);

    leader.timeout();
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = leader.current_term();
    v.vote_granted = true;
    v.to = 1;
    v.from = 2;
    leader.receive(v);
    v.from = 3;
    leader.receive(v);
    leader.become_leader();
    t.clear();

    leader.client_request("a");
    leader.client_request("b");
    replicate_and_commit(leader, f2, f3, t);

    REQUIRE(leader.commit_index() == 2);
    REQUIRE(leader.log().size() == 2);

    leader.compact();

    CHECK(leader.snapshot_index() == 2);
    CHECK(leader.log().empty());
}

TEST_CASE("auto-compact triggers when log"
          " exceeds threshold") {
    memory_transport t;
    server<memory_transport> leader(1, {2, 3}, t);
    server<memory_transport> f2(2, {1, 3}, t);
    server<memory_transport> f3(3, {1, 2}, t);

    leader.timeout();
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = leader.current_term();
    v.vote_granted = true;
    v.to = 1;
    v.from = 2;
    leader.receive(v);
    v.from = 3;
    leader.receive(v);
    leader.become_leader();
    t.clear();

    // threshold of 2: compact fires once log
    // reaches 2 committed entries
    leader.set_compact_threshold(2);

    leader.client_request("a");
    leader.client_request("b");
    replicate_and_commit(leader, f2, f3, t);

    // advance_commit_index calls apply_committed
    // which auto-triggers compact()
    leader.advance_commit_index();

    CHECK(leader.snapshot_index() == 2);
    CHECK(leader.log().empty());
}

TEST_CASE("compact keeps entries after commit") {
    memory_transport t;
    server<memory_transport> leader(1, {2, 3}, t);
    server<memory_transport> f2(2, {1, 3}, t);
    server<memory_transport> f3(3, {1, 2}, t);

    leader.timeout();
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = leader.current_term();
    v.vote_granted = true;
    v.to = 1;
    v.from = 2;
    leader.receive(v);
    v.from = 3;
    leader.receive(v);
    leader.become_leader();
    t.clear();

    leader.client_request("a");
    replicate_and_commit(leader, f2, f3, t);

    // add uncommitted entry
    leader.client_request("b");

    REQUIRE(leader.commit_index() == 1);
    REQUIRE(leader.log().size() == 2);

    leader.compact();

    // "b" at log index 2 should remain
    CHECK(leader.snapshot_index() == 1);
    CHECK(leader.log().size() == 1);
    CHECK(leader.log()[0].value == "b");
}

// -------------------------------------------------------
// InstallSnapshot RPC
// -------------------------------------------------------

TEST_CASE("leader sends InstallSnapshot when"
          " follower lags") {
    memory_transport t;
    server<memory_transport> leader(1, {2, 3}, t);
    server<memory_transport> f2(2, {1, 3}, t);
    server<memory_transport> f3(3, {1, 2}, t);

    leader.timeout();
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = leader.current_term();
    v.vote_granted = true;
    v.to = 1;
    v.from = 2;
    leader.receive(v);
    v.from = 3;
    leader.receive(v);
    leader.become_leader();
    t.clear();

    leader.client_request("x");
    replicate_and_commit(leader, f2, f3, t);
    leader.compact();

    REQUIRE(leader.snapshot_index() == 1);

    // add a new peer that's behind
    server<memory_transport> f4(4, {1, 2, 3}, t);
    leader.add_peer(4);

    // first append_entries: next_index_[4]=2,
    // leader sends AE (prev=1); f4 rejects since
    // it has no log and no snapshot
    leader.append_entries(4);
    deliver(t, [&](const message& m) {
        if (m.to == 4)
            f4.receive(m);
    });
    // f4 sends failure response; leader decrements
    // next_index_[4] to 1
    deliver(t, [&](const message& m) {
        if (m.to == 1)
            leader.receive(m);
    });

    // second append_entries: next_index_[4]=1
    // <= snapshot_index_=1 → InstallSnapshot
    leader.append_entries(4);

    REQUIRE(t.sent.size() >= 1);
    bool found_snap = false;
    for (auto& m : t.sent) {
        if (m.type == msg_type::install_snapshot_req) {
            found_snap = true;
            CHECK(m.to == 4);
            CHECK(m.snapshot_index.value() == 1);
        }
    }
    CHECK(found_snap);

    // deliver snapshot to f4
    deliver(t, [&](const message& m) {
        if (m.to == 4)
            f4.receive(m);
    });

    CHECK(f4.snapshot_index() == 1);
    CHECK(f4.commit_index() == 1);

    // leader processes InstallSnapshot response
    deliver(t, [&](const message& m) {
        if (m.to == 1)
            leader.receive(m);
    });
    CHECK(leader.next_index_for(4) == 2);
    CHECK(leader.match_index_for(4) == 1);
}

TEST_CASE("follower installs snapshot and"
          " catches up") {
    memory_transport t;
    server<memory_transport> leader(1, {2}, t);
    server<memory_transport> f2(2, {1}, t);

    leader.timeout();
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = leader.current_term();
    v.vote_granted = true;
    v.to = 1;
    v.from = 2;
    leader.receive(v);
    leader.become_leader();
    t.clear();

    leader.client_request("x");
    leader.client_request("y");

    // replicate both to follower
    leader.append_entries(2);
    deliver(t, [&](const message& m) {
        if (m.to == 2)
            f2.receive(m);
    });
    deliver(t, [&](const message& m) {
        if (m.to == 1)
            leader.receive(m);
    });
    leader.advance_commit_index();
    leader.append_entries(2);
    deliver(t, [&](const message& m) {
        if (m.to == 2)
            f2.receive(m);
    });
    t.clear();

    REQUIRE(f2.commit_index() == 2);
    REQUIRE(f2.log().size() == 2);

    leader.compact();
    REQUIRE(leader.snapshot_index() == 2);

    // state machine should have applied entries
    auto& sm = leader.state_machine();
    CHECK(sm.applied.size() == 2);

    // send snapshot to follower
    message snap_msg;
    snap_msg.type = msg_type::install_snapshot_req;
    snap_msg.term = leader.current_term();
    snap_msg.from = 1;
    snap_msg.to = 2;
    snap_msg.snapshot_index = leader.snapshot_index();
    snap_msg.snapshot_term = 0;
    snap_msg.snapshot_data = leader.state_machine().snapshot();
    f2.receive(snap_msg);

    CHECK(f2.snapshot_index() == 2);
    CHECK(f2.log().empty());
}

// -------------------------------------------------------
// joint consensus tests
// -------------------------------------------------------

TEST_CASE("config_request appends config_joint"
          " entry") {
    memory_transport t;
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

    s.config_request({2, 3, 4});

    REQUIRE(s.log().size() == 1);
    CHECK(s.log()[0].type == entry_type::config_joint);
    REQUIRE(s.joint_config().has_value());
    CHECK(s.joint_config()->count(4) == 1);
    CHECK(s.peers().count(4) == 1);
}

TEST_CASE("joint consensus: C_new committed"
          " updates peers") {
    memory_transport t;
    server<memory_transport> leader(1, {2, 3}, t);
    server<memory_transport> f2(2, {1, 3}, t);
    server<memory_transport> f3(3, {1, 2}, t);

    // elect leader
    leader.timeout();
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = leader.current_term();
    v.vote_granted = true;
    v.to = 1;
    v.from = 2;
    leader.receive(v);
    v.from = 3;
    leader.receive(v);
    leader.become_leader();
    t.clear();

    // initiate membership change: add peer 4
    // new_peers (excluding self 1) = {2, 3, 4}
    leader.config_request({2, 3, 4});

    // replicate config_joint to f2 and f3
    leader.append_entries(2);
    leader.append_entries(3);
    deliver(t, [&](const message& m) {
        if (m.to == 2)
            f2.receive(m);
        if (m.to == 3)
            f3.receive(m);
    });
    // collect acks
    deliver(t, [&](const message& m) {
        if (m.to == 1)
            leader.receive(m);
    });

    // commit config_joint — leader appends
    // config_final automatically in apply_committed
    leader.advance_commit_index();

    // log now has: [config_joint, config_final]
    REQUIRE(leader.log().size() == 2);
    CHECK(leader.log()[1].type == entry_type::config_final);

    // replicate config_final to peers
    leader.append_entries(2);
    leader.append_entries(3);
    deliver(t, [&](const message& m) {
        if (m.to == 2)
            f2.receive(m);
        if (m.to == 3)
            f3.receive(m);
    });
    deliver(t, [&](const message& m) {
        if (m.to == 1)
            leader.receive(m);
    });

    // advance commit to config_final index
    leader.advance_commit_index();

    // joint_config_ should be cleared
    CHECK(!leader.joint_config().has_value());
    // peers_ should now be {2, 3, 4}
    CHECK(leader.peers().count(2) == 1);
    CHECK(leader.peers().count(3) == 1);
    CHECK(leader.peers().count(4) == 1);
    CHECK(leader.peers().size() == 3);
}
