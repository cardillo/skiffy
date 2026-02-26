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

TEST_CASE("leader appends client request to log") {
    memory_transport t;
    auto s = make_leader(t);

    s.client_request("cmd1");
    REQUIRE(s.log().size() == 1);
    CHECK(s.log()[0].term == s.current_term());
    CHECK(s.log()[0].value == "cmd1");
}

TEST_CASE("multiple client requests append in order") {
    memory_transport t;
    auto s = make_leader(t);

    s.client_request("a");
    s.client_request("b");
    s.client_request("c");
    REQUIRE(s.log().size() == 3);
    CHECK(s.log()[0].value == "a");
    CHECK(s.log()[1].value == "b");
    CHECK(s.log()[2].value == "c");
}

TEST_CASE("client_request is no-op for follower") {
    memory_transport t;
    server s(1, {2, 3}, t);

    s.client_request("x");
    CHECK(s.log().empty());
}

TEST_CASE("client_request is no-op for candidate") {
    memory_transport t;
    server s(1, {2, 3}, t);
    s.timeout();

    s.client_request("x");
    CHECK(s.log().empty());
}
