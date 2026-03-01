#include "doctest/doctest.h"

#include "raftpp.h"
#include "test_utils.h"

using namespace raftpp;

static message make_msg(server_id src, server_id dst) {
    message m;
    m.type = msg_type::request_vote_req;
    m.term = 1;
    m.from = src;
    m.to = dst;
    m.last_log_term = 0;
    m.last_log_index = 0;
    return m;
}

TEST_CASE("sim_transport delivers all messages by default") {
    sim_transport t;
    t.send(make_msg(s1, s2));
    t.send(make_msg(s1, s3));

    std::vector<message> received;
    t.deliver([&](const message& m) { received.push_back(m); });

    CHECK(received.size() == 2);
    CHECK(t.pending().empty());
}

TEST_CASE("sim_transport drop_rate=1 drops all") {
    sim_transport t;
    t.drop_rate = 1.0;
    t.send(make_msg(s1, s2));
    t.send(make_msg(s1, s3));

    std::vector<message> received;
    t.deliver([&](const message& m) { received.push_back(m); });

    CHECK(received.empty());
}

TEST_CASE("sim_transport dup_rate=1 duplicates all") {
    sim_transport t;
    t.dup_rate = 1.0;
    t.send(make_msg(s1, s2));

    std::vector<message> received;
    t.deliver([&](const message& m) { received.push_back(m); });

    CHECK(received.size() == 2);
}

TEST_CASE("sim_transport clear empties queue") {
    sim_transport t;
    t.send(make_msg(s1, s2));
    t.clear();
    CHECK(t.pending().empty());
}

TEST_CASE("sim_transport deliver is a one-shot flush") {
    sim_transport t;
    t.send(make_msg(s1, s2));

    int count = 0;
    t.deliver([&](const message&) { ++count; });
    CHECK(count == 1);

    // second deliver sees nothing
    t.deliver([&](const message&) { ++count; });
    CHECK(count == 1);
}

TEST_CASE("sim_transport: server-level integration") {
    sim_transport t;
    server<sim_transport> sv1(s1, {s2, s3}, t);
    server<sim_transport> sv2(s2, {s1, s3}, t);
    server<sim_transport> sv3(s3, {s1, s2}, t);

    // s1 times out and sends RequestVote to s2 and s3
    sv1.timeout();
    sv1.request_vote(s2);
    sv1.request_vote(s3);
    CHECK(t.pending().size() == 2);

    // deliver votes to s2 and s3, they reply
    t.deliver([&](const message& m) {
        if (m.to == s2)
            sv2.receive(m);
        if (m.to == s3)
            sv3.receive(m);
    });
    // s2 and s3 each sent a vote response
    CHECK(t.pending().size() == 2);

    // deliver responses back to s1
    t.deliver([&](const message& m) { sv1.receive(m); });

    sv1.become_leader();
    CHECK(sv1.state() == server_state::leader);
}
