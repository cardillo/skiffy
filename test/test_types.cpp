#include "doctest/doctest.h"

#include "raftpp.h"
#include "test_utils.h"

using namespace raftpp;

TEST_CASE("log_entry equality") {
    log_entry a{1, entry_type::data, "x"};
    log_entry b{1, entry_type::data, "x"};
    log_entry c{2, entry_type::data, "x"};
    CHECK(a == b);
    CHECK(a != c);
}

TEST_CASE("message default fields") {
    message m;
    m.type = msg_type::request_vote_req;
    m.term = 1;
    m.from = s1;
    m.to = s2;
    CHECK(m.term == 1);
    CHECK(m.from == s1);
    CHECK_FALSE(m.vote_granted.has_value());
    CHECK_FALSE(m.success.has_value());
}

TEST_CASE("message equality") {
    message a;
    a.type = msg_type::request_vote_req;
    a.term = 1;
    a.from = s1;
    a.to = s2;
    a.last_log_term = 0;
    a.last_log_index = 0;

    message b = a;
    CHECK(a == b);

    b.term = 2;
    CHECK(a != b);
}

TEST_CASE("server_state values") {
    CHECK(server_state::follower != server_state::candidate);
    CHECK(server_state::candidate != server_state::leader);
}
