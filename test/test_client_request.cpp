#include "doctest/doctest.h"

#include "skiffy.hpp"
#include "test_utils.h"

using namespace skiffy;

TEST_CASE("leader appends client request to log") {
    memory_transport t;
    test_server<memory_transport> s(s1, {s2, s3}, t);
    make_leader(s, t);

    s.client_request("cmd1");
    REQUIRE(s.log().size() == 1);
    CHECK(s.log()[0].term == s.current_term());
    CHECK(s.log()[0].value == "cmd1");
}

TEST_CASE("multiple client requests append in order") {
    memory_transport t;
    test_server<memory_transport> s(s1, {s2, s3}, t);
    make_leader(s, t);

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
    test_server s(s1, {s2, s3}, t);

    s.client_request("x");
    CHECK(s.log().empty());
}

TEST_CASE("client_request is no-op for candidate") {
    memory_transport t;
    test_server s(s1, {s2, s3}, t);
    s.timeout();

    s.client_request("x");
    CHECK(s.log().empty());
}
