#include "doctest/doctest.h"

#include "skiffy.hpp"
#include "test_utils.h"

using namespace skiffy;

// -------------------------------------------------------
// helpers
// -------------------------------------------------------

// run up to max_rounds steps until all_committed(idx)
static bool converge(cluster_sim& c, index_t idx, int max_rounds = 60) {
    for (int i = 0; i < max_rounds; ++i) {
        c.broadcast();
        c.step();
        c.step();
        c.advance();
        if (c.all_committed(idx))
            return true;
    }
    return false;
}

// -------------------------------------------------------
// election under packet loss
// -------------------------------------------------------

TEST_CASE("election succeeds with 30% packet loss") {
    cluster_sim c({s1, s2, s3});
    c.transport.drop_rate = 0.3;
    skiffy::node_id lid = c.elect_leader(60);
    CHECK(!lid.is_nil());
}

TEST_CASE("election succeeds with 50% packet loss") {
    cluster_sim c({s1, s2, s3, s4, s5});
    c.transport.drop_rate = 0.5;
    node_id lid = c.elect_leader(200);
    CHECK(!lid.is_nil());
}

TEST_CASE("duplicate messages do not corrupt election") {
    cluster_sim c({s1, s2, s3});
    c.transport.dup_rate = 1.0; // every message doubled
    node_id lid = c.elect_leader(30);
    CHECK(!lid.is_nil());
    // exactly one leader must exist
    CHECK(c.leaders().size() == 1);
}

// -------------------------------------------------------
// replication under packet loss
// -------------------------------------------------------

TEST_CASE("log replication converges with 20% loss") {
    cluster_sim c({s1, s2, s3});
    REQUIRE(!c.elect_leader().is_nil());

    c.transport.drop_rate = 0.2;
    c.submit("x");
    CHECK(converge(c, 1));
}

TEST_CASE("multiple entries replicate with 30% loss") {
    cluster_sim c({s1, s2, s3});
    REQUIRE(!c.elect_leader().is_nil());

    c.transport.drop_rate = 0.3;
    for (int i = 0; i < 5; ++i)
        c.submit(std::to_string(i));

    CHECK(converge(c, 5, 120));
}

TEST_CASE("duplicate + drop: replication still converges") {
    cluster_sim c({s1, s2, s3});
    REQUIRE(!c.elect_leader().is_nil());

    c.transport.drop_rate = 0.2;
    c.transport.dup_rate = 0.3;
    c.submit("chaos");
    CHECK(converge(c, 1));
}

// -------------------------------------------------------
// follower crash
// -------------------------------------------------------

TEST_CASE("cluster makes progress with one crashed follower") {
    cluster_sim c({s1, s2, s3});
    node_id lid = c.elect_leader();
    REQUIRE(!lid.is_nil());

    // crash a follower — majority (2/3) still available
    node_id crashed_id = nil_id;
    for (auto& [id, _] : c.nodes)
        if (id != lid) {
            crashed_id = id;
            break;
        }
    c.crash(crashed_id);

    c.submit("a");
    c.submit("b");
    CHECK(converge(c, 2));
}

TEST_CASE("crashed follower catches up after recovery") {
    cluster_sim c({s1, s2, s3});
    node_id lid = c.elect_leader();
    REQUIRE(!lid.is_nil());

    node_id lagging = nil_id;
    for (auto& [id, _] : c.nodes)
        if (id != lid) {
            lagging = id;
            break;
        }

    // crash, replicate some entries, then recover
    c.crash(lagging);
    c.submit("a");
    c.submit("b");
    converge(c, 2, 30); // commit on the live majority

    c.recover(lagging);

    // after recovery the lagging node should catch up
    CHECK(converge(c, 2, 60));
    CHECK(c.nodes.at(lagging)->commit_index() >= 2);
}

// -------------------------------------------------------
// leader crash
// -------------------------------------------------------

TEST_CASE("new election after leader crash") {
    cluster_sim c({s1, s2, s3});
    node_id old_lid = c.elect_leader();
    REQUIRE(!old_lid.is_nil());

    c.crash(old_lid);

    // remaining nodes should be able to elect a new leader
    node_id new_lid = c.elect_leader(60);
    CHECK(!new_lid.is_nil());
    CHECK(new_lid != old_lid);
}

TEST_CASE("entries committed before leader crash survive") {
    cluster_sim c({s1, s2, s3});
    node_id lid = c.elect_leader();
    REQUIRE(!lid.is_nil());

    c.submit("committed-entry");
    REQUIRE(converge(c, 1));

    // find a follower that has the entry
    node_id follower = nil_id;
    for (auto& [id, _] : c.nodes)
        if (id != lid) {
            follower = id;
            break;
        }

    c.crash(lid);
    node_id new_lid = c.elect_leader(60);
    REQUIRE(!new_lid.is_nil());

    // entry must still be in the log on surviving nodes
    CHECK(c.nodes.at(new_lid)->log().size() >= 1);
    CHECK(c.nodes.at(follower)->log().size() >= 1);
}

TEST_CASE("5-node cluster survives two simultaneous crashes") {
    cluster_sim c({s1, s2, s3, s4, s5});
    node_id lid = c.elect_leader(60);
    REQUIRE(!lid.is_nil());

    // crash two followers (majority 3/5 remains)
    int count = 0;
    for (auto& [id, _] : c.nodes) {
        if (id != lid && count < 2) {
            c.crash(id);
            ++count;
        }
    }

    c.submit("x");
    CHECK(converge(c, 1, 60));
}

// -------------------------------------------------------
// network partition
// -------------------------------------------------------

TEST_CASE("minority partition cannot elect a leader") {
    // 5-node cluster; isolate one follower
    cluster_sim c({s1, s2, s3, s4, s5});
    node_id lid = c.elect_leader(60);
    REQUIRE(!lid.is_nil());

    // pick a follower (not the leader) as the minority
    node_id minority_node = nil_id;
    for (auto& [id, _] : c.nodes)
        if (id != lid) {
            minority_node = id;
            break;
        }

    std::set<node_id> maj;
    for (auto& [id, _] : c.nodes)
        if (id != minority_node)
            maj.insert(id);
    c.partition_cluster({minority_node}, maj);

    // force isolated node to start an election;
    // its votes are all dropped by the partition
    c.nodes.at(minority_node)->timeout();
    for (auto& [pid, _] : c.nodes)
        if (pid != minority_node)
            c.nodes.at(minority_node)->request_vote(pid);

    c.run(20);

    // 1 of 5 nodes cannot satisfy quorum (need 3)
    CHECK(c.nodes.at(minority_node)->state() != detail::server_state::leader);
}

TEST_CASE("majority partition makes progress, minority stalls") {
    cluster_sim c({s1, s2, s3, s4, s5});
    node_id lid = c.elect_leader(60);
    REQUIRE(!lid.is_nil());

    // leader + 2 followers in majority; 2 in minority
    std::set<node_id> majority, minority;
    for (auto& [id, _] : c.nodes) {
        if (id == lid || majority.size() < 3)
            majority.insert(id);
        else
            minority.insert(id);
    }
    c.partition_cluster(majority, minority);

    c.submit("during-partition");

    // drive replication within the majority partition;
    // check only majority nodes, not the whole cluster
    bool maj_committed = false;
    for (int i = 0; i < 60 && !maj_committed; ++i) {
        c.broadcast();
        c.step();
        c.step();
        c.advance();
        maj_committed = true;
        for (auto id : majority)
            if (c.nodes.at(id)->commit_index() < 1)
                maj_committed = false;
    }
    CHECK(maj_committed);

    // minority nodes must still be at index 0
    for (auto id : minority)
        CHECK(c.nodes.at(id)->commit_index() == 0);
}

TEST_CASE("partition heals: minority catches up") {
    cluster_sim c({s1, s2, s3});
    node_id lid = c.elect_leader();
    REQUIRE(!lid.is_nil());

    // isolate one follower
    node_id isolated = nil_id;
    node_id third = nil_id;
    for (auto& [id, _] : c.nodes) {
        if (id != lid && isolated.is_nil())
            isolated = id;
        else if (id != lid)
            third = id;
    }

    c.partition_cluster({isolated}, {lid, third});

    c.submit("pre-heal");

    // drive replication in majority {lid, third}
    bool maj_committed = false;
    for (int i = 0; i < 30 && !maj_committed; ++i) {
        c.broadcast();
        c.step();
        c.step();
        c.advance();
        maj_committed = c.nodes.at(lid)->commit_index() >= 1 &&
            c.nodes.at(third)->commit_index() >= 1;
    }
    REQUIRE(maj_committed);

    c.heal();
    // after heal the isolated node catches up via AE
    CHECK(converge(c, 1, 60));
}

// -------------------------------------------------------
// membership changes under faults
// -------------------------------------------------------

TEST_CASE("membership change completes with 20% packet loss") {
    cluster_sim c({s1, s2, s3});
    node_id lid = c.elect_leader();
    REQUIRE(!lid.is_nil());

    c.transport.drop_rate = 0.2;

    // add node 4 to the cluster via joint consensus
    auto* ldr = c.nodes.at(lid).get();
    std::set<node_id> new_peers;
    for (auto& [id, _] : c.nodes)
        if (id != lid)
            new_peers.insert(id);
    new_peers.insert(s4);

    // add node 4 to transport routing
    std::set<node_id> peers4;
    for (auto& [id, _] : c.nodes)
        peers4.insert(id);
    c.nodes[s4] = std::make_unique<detail::test_server<sim_transport>>(
        s4, peers4, c.transport);

    ldr->config_request(new_peers);

    // run enough rounds for joint + final to commit
    CHECK(converge(c, 2, 120));
    CHECK(!ldr->joint_config().has_value());
}

TEST_CASE("membership change survives leader crash mid-flight") {
    cluster_sim c({s1, s2, s3});
    node_id lid = c.elect_leader();
    REQUIRE(!lid.is_nil());

    auto* ldr = c.nodes.at(lid).get();
    // initiate config change (removes node 3, adds nobody)
    // new config = {lid, other_follower}
    node_id other = nil_id;
    for (auto& [id, _] : c.nodes)
        if (id != lid && id != s3) {
            other = id;
            break;
        }

    ldr->config_request({other});
    REQUIRE(ldr->joint_config().has_value());

    // crash the leader before config_joint is replicated
    c.crash(lid);
    c.transport.clear();

    // surviving nodes elect a new leader
    node_id new_lid = c.elect_leader(60);
    REQUIRE(!new_lid.is_nil());
    REQUIRE(new_lid != lid);

    // new leader appends config_final to resolve joint config
    // (the cluster must reach a stable config)
    converge(c, 1, 60);
    CHECK(!c.nodes.at(new_lid)->joint_config().has_value());
}
