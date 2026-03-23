#include "doctest/doctest.h"

#include "skiffy.hpp"
#include "test_utils.h"

using skiffy::node_id;

// -------------------------------------------------------
// server::remove_peer
// -------------------------------------------------------

TEST_CASE("remove_peer cleans up server state") {
    skiffy::memory_transport t;
    skiffy::detail::test_server<skiffy::memory_transport> s(s1, {s2, s3}, t);

    REQUIRE(s.peers().count(s2) == 1);
    REQUIRE(s.peers().count(s3) == 1);

    // become leader so next_index / match_index
    // are initialised for all peers
    s.timeout();
    auto grant = [&](skiffy::node_id src) {
        skiffy::message rv;
        rv.type = skiffy::msg_type::request_vote_resp;
        rv.term = s.current_term();
        rv.from = src;
        rv.to = s1;
        rv.vote_granted = true;
        s.receive(rv);
    };
    grant(s2);
    grant(s3);
    s.become_leader();
    REQUIRE(s.state() == skiffy::detail::server_state::leader);

    s.remove_peer(s2);

    CHECK(s.peers().count(s2) == 0);
    CHECK(s.peers().count(s3) == 1);
    // next_index / match_index gone for peer s2
    bool threw2 = false;
    try {
        s.next_index_for(s2);
    } catch (...) { threw2 = true; }
    CHECK(threw2);

    bool threw3 = false;
    try {
        s.next_index_for(s3);
    } catch (...) { threw3 = true; }
    CHECK(!threw3);
}

// -------------------------------------------------------
// membership_manager leave/remove flow
// -------------------------------------------------------

// Helper: feed a list of messages into a manager and
// collect all replies.
static std::vector<skiffy::message>
deliver(const std::vector<skiffy::message>& msgs,
        skiffy::detail::membership_manager& dst) {
    std::vector<skiffy::message> out;
    for (auto& m : msgs) {
        auto r = dst.receive(m);
        out.insert(out.end(), r.begin(), r.end());
    }
    return out;
}

TEST_CASE("membership_manager handles remove") {
    skiffy::detail::membership_manager mgrA(s1);

    // add s2 via announce
    skiffy::message ann;
    ann.type = skiffy::msg_type::mem_announce;
    ann.from = s2;
    ann.to = s1;
    {
        msgpack::sbuffer buf;
        msgpack::pack(buf, s2);
        ann.payload = std::string(buf.data(), buf.data() + buf.size());
    }
    mgrA.receive(ann);
    REQUIRE(mgrA.members().size() == 2);

    node_id removed_id;
    mgrA.on_peer_removed([&](node_id id) { removed_id = id; });

    // send remove message for s2
    skiffy::message rm;
    rm.type = skiffy::msg_type::mem_remove;
    rm.from = s3;
    rm.to = s1;
    {
        msgpack::sbuffer buf;
        msgpack::pack(buf, s2);
        rm.payload = std::string(buf.data(), buf.data() + buf.size());
    }
    mgrA.receive(rm);

    CHECK(removed_id == s2);
    bool found = false;
    for (auto& m : mgrA.members())
        if (m == s2)
            found = true;
    CHECK(!found);
}

TEST_CASE("membership_manager notify_leave removes self from peers") {
    skiffy::detail::membership_manager mgr1(s1);
    skiffy::detail::membership_manager mgr2(s2);

    // join flow to set up membership
    auto req = mgr2.join(s1);
    auto resps = mgr1.receive(req);
    auto anns = mgr2.receive(resps[0]);
    deliver(anns, mgr1);

    REQUIRE(mgr1.members().size() == 2);
    REQUIRE(mgr2.members().size() == 2);

    node_id removed_on_1;
    mgr1.on_peer_removed([&](node_id id) { removed_on_1 = id; });

    // mgr2 leaves
    auto leave_msgs = mgr2.notify_leave();
    deliver(leave_msgs, mgr1);

    CHECK(removed_on_1 == s2);
}
