#include "doctest/doctest.h"

#include "skiffy.hpp"
#include "test_utils.h"

using namespace skiffy;

// -------------------------------------------------------
// detail::membership_manager unit tests
// -------------------------------------------------------

// Helper: feed a list of messages into a manager and
// collect all replies.
static std::vector<message> deliver(const std::vector<message>& msgs,
                                    detail::membership_manager& dst) {
    std::vector<message> out;
    for (auto& m : msgs) {
        auto r = dst.receive(m);
        out.insert(out.end(), r.begin(), r.end());
    }
    return out;
}

TEST_CASE("detail::membership_manager: join flow") {
    // bootstrap node
    detail::membership_manager mgr1(s1);

    // joining node
    detail::membership_manager mgr2(s2);

    // step 1: joiner sends mem_join_req
    auto req = mgr2.join(s1);
    CHECK(req.type == msg_type::mem_join_req);

    // step 2: bootstrap handles join_req, sends join_resp
    auto resps = mgr1.receive(req);
    REQUIRE(resps.size() == 1);
    CHECK(resps[0].type == msg_type::mem_join_resp);

    // step 3: joiner handles join_resp, sends announce
    auto anns = mgr2.receive(resps[0]);
    CHECK(mgr2.members().size() == 2);
    REQUIRE(anns.size() == 1);
    CHECK(anns[0].type == msg_type::mem_announce);

    // step 4: bootstrap handles announce
    deliver(anns, mgr1);
    CHECK(mgr1.members().size() == 2);
}

TEST_CASE("detail::membership_manager: on_peer_added fires on joiner") {
    detail::membership_manager mgr1(s1);
    detail::membership_manager mgr2(s2);

    node_id added_on_2;
    mgr2.on_peer_added([&](node_id id) { added_on_2 = id; });

    auto req = mgr2.join(s1);
    auto resps = mgr1.receive(req);
    mgr2.receive(resps[0]);

    CHECK(added_on_2 == s1);
}

TEST_CASE("detail::membership_manager: on_peer_added fires on bootstrap") {
    detail::membership_manager mgr1(s1);
    detail::membership_manager mgr2(s2);

    node_id added_on_1;
    mgr1.on_peer_added([&](node_id id) { added_on_1 = id; });

    auto req = mgr2.join(s1);
    mgr1.receive(req);

    CHECK(added_on_1 == s2);
}

TEST_CASE("detail::membership_manager: duplicate announce ignored") {
    detail::membership_manager mgr1(s1);

    // manually send two announces for s2
    message ann;
    ann.type = msg_type::mem_announce;
    ann.from = s2;
    ann.to = s1;
    msgpack::sbuffer buf;
    msgpack::pack(buf, s2);
    ann.payload = std::string(buf.data(), buf.data() + buf.size());

    mgr1.receive(ann);
    mgr1.receive(ann); // duplicate

    int n = 0;
    for (auto& m : mgr1.members())
        if (m == s2)
            ++n;
    CHECK(n == 1);
}

TEST_CASE("detail::membership_manager: remove fires on_peer_removed") {
    detail::membership_manager mgr1(s1);

    // add s2 via announce
    message ann;
    ann.type = msg_type::mem_announce;
    ann.from = s2;
    ann.to = s1;
    {
        msgpack::sbuffer buf;
        msgpack::pack(buf, s2);
        ann.payload = std::string(buf.data(), buf.data() + buf.size());
    }
    mgr1.receive(ann);
    REQUIRE(mgr1.members().size() == 2);

    node_id removed;
    mgr1.on_peer_removed([&](node_id id) { removed = id; });

    // send mem_remove for s2
    message rm;
    rm.type = msg_type::mem_remove;
    rm.from = s3;
    rm.to = s1;
    {
        msgpack::sbuffer buf;
        msgpack::pack(buf, s2);
        rm.payload = std::string(buf.data(), buf.data() + buf.size());
    }
    mgr1.receive(rm);

    CHECK(removed == s2);
    CHECK(mgr1.members().size() == 1);
}

TEST_CASE(
    "detail::membership_manager: notify_leave sends mem_remove to all") {
    detail::membership_manager mgr1(s1);

    // add s2 and s3
    auto add_member = [&](node_id id) {
        message ann;
        ann.type = msg_type::mem_announce;
        ann.from = id;
        ann.to = s1;
        msgpack::sbuffer buf;
        msgpack::pack(buf, id);
        ann.payload = std::string(buf.data(), buf.data() + buf.size());
        mgr1.receive(ann);
    };
    add_member(s2);
    add_member(s3);

    auto msgs = mgr1.notify_leave();

    // should send mem_remove to s2 and s3
    CHECK(msgs.size() == 2);
    for (auto& m : msgs)
        CHECK(m.type == msg_type::mem_remove);
}

// -------------------------------------------------------
// server::add_peer tests
// -------------------------------------------------------

TEST_CASE("add_peer grows peer set") {
    memory_transport t;
    detail::test_server<memory_transport> s(s1, {}, t);

    CHECK(s.peers().empty());
    s.add_peer(s2);
    CHECK(s.peers().size() == 1);
    CHECK(s.peers().count(s2) == 1);
    s.add_peer(s3);
    CHECK(s.peers().size() == 2);
}

TEST_CASE("add_peer initialises leader vars") {
    memory_transport t;
    detail::test_server<memory_transport> s(s1, {}, t);
    s.add_peer(s2);

    CHECK(s.next_index_for(s2) == 1);
    CHECK(s.match_index_for(s2) == 0);
}

TEST_CASE("add_peer after log entries: next_index correct") {
    memory_transport t;
    // 3-node cluster: need 2 votes (majority of 3)
    detail::test_server<memory_transport> s(s1, {s2, s3}, t);

    s.timeout();

    auto grant = [&](node_id src) {
        message rv;
        rv.type = msg_type::request_vote_resp;
        rv.term = s.current_term();
        rv.from = src;
        rv.to = s1;
        rv.vote_granted = true;
        s.receive(rv);
    };
    grant(s2);
    grant(s3);

    s.become_leader();
    REQUIRE(s.state() == detail::server_state::leader);

    s.client_request("x");
    // log now has 1 entry; add a 4th peer
    s.add_peer(s4);

    CHECK(s.next_index_for(s4) == 2);
    CHECK(s.match_index_for(s4) == 0);
}

TEST_CASE("quorum recalculates after add_peer") {
    memory_transport t;
    // start solo: cluster size=1, any 1 vote is quorum
    detail::test_server<memory_transport> s(s1, {}, t);

    std::set<node_id> just_self{s1};
    CHECK(s.is_quorum(just_self));

    s.add_peer(s2);
    // cluster size=2: need >1 vote, so {s1} is not quorum
    CHECK(!s.is_quorum(just_self));

    std::set<node_id> both{s1, s2};
    CHECK(s.is_quorum(both));
}

TEST_CASE("leader crash before config_joint"
          " committed reverts membership") {
    memory_transport t;
    detail::test_server<memory_transport> sv1(s1, {s2, s3}, t);
    detail::test_server<memory_transport> sv2(s2, {s1, s3}, t);
    detail::test_server<memory_transport> sv3(s3, {s1, s2}, t);

    // elect s1 as leader (term 2)
    sv1.timeout();
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = sv1.current_term();
    v.vote_granted = true;
    v.to = s1;
    v.from = s2;
    sv1.receive(v);
    v.from = s3;
    sv1.receive(v);
    sv1.become_leader();
    t.clear();

    // initiate config change before replicating
    sv1.config_request({s2, s3, s4});
    REQUIRE(sv1.joint_config().has_value());
    REQUIRE(sv1.peers().count(s4) == 1);

    // s1 crashes before config_joint is
    // replicated or committed
    sv1.restart();
    t.clear();

    // s2 wins election at term 3 with a
    // client entry (term=3) that will conflict
    // with s1's config_joint (term=2)
    sv2.timeout(); // term 2
    sv2.timeout(); // term 3
    message v2;
    v2.type = msg_type::request_vote_resp;
    v2.term = sv2.current_term();
    v2.vote_granted = true;
    v2.to = s2;
    v2.from = s3;
    sv2.receive(v2);
    sv2.become_leader();
    t.clear();

    sv2.client_request("x");
    sv2.append_entries(s1);
    // deliver s2's AE to s1; s1 truncates the
    // stale config_joint entry on conflict
    t.deliver([&](const message& m) {
        if (m.to == s1)
            sv1.receive(m);
    });

    CHECK(!sv1.joint_config().has_value());
    CHECK(!sv1.peers().count(s4));
    CHECK(sv1.peers().count(s2) == 1);
    CHECK(sv1.peers().count(s3) == 1);
}

TEST_CASE("new leader after crash re-appends"
          " config_final") {
    memory_transport t;
    // old config: {1,2,3}; new config: {2,3}
    detail::test_server<memory_transport> sv2(s2, {s1, s3}, t);

    // step 1: s2 receives a committed AE from s1
    // (term 2) with config_joint{2,3} at index 1
    msgpack::sbuffer buf;
    std::set<node_id> nc{s2, s3};
    msgpack::pack(buf, nc);
    log_entry cj;
    cj.term = 2;
    cj.type = entry_type::config_joint;
    cj.value = std::string(buf.data(), buf.data() + buf.size());

    message ae;
    ae.type = msg_type::append_entries_req;
    ae.term = 2;
    ae.from = s1;
    ae.to = s2;
    ae.prev_log_index = 0;
    ae.prev_log_term = 0;
    ae.entries = std::vector<log_entry>{cj};
    ae.commit_index = 1;
    sv2.receive(ae);

    REQUIRE(sv2.joint_config().has_value());
    REQUIRE(sv2.log().size() == 1);
    REQUIRE(sv2.log()[0].type == entry_type::config_joint);
    t.clear();

    // step 2: s2 wins election at term 3 with
    // one vote from s3; joint quorum is satisfied
    sv2.timeout(); // term 3
    t.clear();

    message rv;
    rv.type = msg_type::request_vote_resp;
    rv.term = sv2.current_term();
    rv.from = s3;
    rv.to = s2;
    rv.vote_granted = true;
    sv2.receive(rv);

    // step 3: become_leader fires the fix —
    // config_final(term=3) appended at index 2
    sv2.become_leader();
    REQUIRE(sv2.state() == detail::server_state::leader);
    REQUIRE(sv2.log().size() == 2);
    CHECK(sv2.log()[1].type == entry_type::config_final);
    CHECK(sv2.log()[1].term == 3);
    t.clear();

    // step 4: fake AE resp from s3 (match_index=2)
    message resp;
    resp.type = msg_type::append_entries_resp;
    resp.term = sv2.current_term();
    resp.from = s3;
    resp.to = s2;
    resp.success = true;
    resp.match_index = 2;
    sv2.receive(resp);

    // step 5: advance_commit_index commits index 2
    sv2.advance_commit_index();

    // joint consensus resolved: s1 removed
    CHECK(!sv2.joint_config().has_value());
    CHECK(sv2.peers().count(s3) == 1);
    CHECK(!sv2.peers().count(s1));
}

TEST_CASE("on_peer_added fires on config"
          " change adding a peer") {
    memory_transport t;
    // follower s2; old config {1,2,3}
    detail::test_server<memory_transport> sv2(s2, {s1, s3}, t);

    node_id added_pid;
    sv2.on_peer_added([&](node_id pid) { added_pid = pid; });

    // build shared encoded new config {2,3,4}
    msgpack::sbuffer buf;
    std::set<node_id> nc{s2, s3, s4};
    msgpack::pack(buf, nc);
    std::string enc(buf.data(), buf.data() + buf.size());

    // AE1: config_joint at index 1, uncommitted
    log_entry cj;
    cj.term = 2;
    cj.type = entry_type::config_joint;
    cj.value = enc;

    message ae1;
    ae1.type = msg_type::append_entries_req;
    ae1.term = 2;
    ae1.from = s1;
    ae1.to = s2;
    ae1.prev_log_index = 0;
    ae1.prev_log_term = 0;
    ae1.entries = std::vector<log_entry>{cj};
    ae1.commit_index = 0;
    sv2.receive(ae1);

    // AE2: config_final at index 2,
    // both entries committed
    log_entry cf;
    cf.term = 2;
    cf.type = entry_type::config_final;
    cf.value = enc;

    message ae2;
    ae2.type = msg_type::append_entries_req;
    ae2.term = 2;
    ae2.from = s1;
    ae2.to = s2;
    ae2.prev_log_index = 1;
    ae2.prev_log_term = 2;
    ae2.entries = std::vector<log_entry>{cf};
    ae2.commit_index = 2;
    sv2.receive(ae2);

    // peer 4 was unknown to s2; callback fires
    CHECK(added_pid == s4);
    // peer 3 was already in peers_ — no callback
    // peer 2 is self — no callback
    CHECK(!sv2.joint_config().has_value());
    CHECK(sv2.peers().count(s4) == 1);
}

TEST_CASE("on_peer_removed fires on config"
          " change removing a peer") {
    memory_transport t;
    // 4-server cluster; config_request({2,3})
    // removes peer 4
    detail::test_server<memory_transport> sv1(s1, {s2, s3, s4}, t);

    node_id removed_pid;
    sv1.on_peer_removed([&](node_id pid) { removed_pid = pid; });

    // elect s1 as leader (term 2)
    sv1.timeout();
    {
        message v;
        v.type = msg_type::request_vote_resp;
        v.term = sv1.current_term();
        v.vote_granted = true;
        v.to = s1;
        v.from = s2;
        sv1.receive(v);
        v.from = s3;
        sv1.receive(v);
    }
    sv1.become_leader();
    t.clear();

    // initiate config change: remove peer 4
    sv1.config_request({s2, s3});
    REQUIRE(sv1.joint_config().has_value());
    // config_joint is at log index 1

    // fake acks from s2 and s3 for index 1
    {
        message resp;
        resp.type = msg_type::append_entries_resp;
        resp.term = sv1.current_term();
        resp.success = true;
        resp.match_index = 1;
        resp.to = s1;
        resp.from = s2;
        sv1.receive(resp);
        resp.from = s3;
        sv1.receive(resp);
    }
    // commit config_joint; leader appends
    // config_final at index 2
    sv1.advance_commit_index();

    // fake acks from s2 and s3 for index 2
    {
        message resp;
        resp.type = msg_type::append_entries_resp;
        resp.term = sv1.current_term();
        resp.success = true;
        resp.match_index = 2;
        resp.to = s1;
        resp.from = s2;
        sv1.receive(resp);
        resp.from = s3;
        sv1.receive(resp);
    }
    // commit config_final → fires on_peer_removed_(4)
    sv1.advance_commit_index();

    CHECK(removed_pid == s4);
    CHECK(!sv1.joint_config().has_value());
    CHECK(!sv1.peers().count(s4));
}
