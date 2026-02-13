#include "doctest/doctest.h"
#include "raftpp.h"

using namespace raftpp;

TEST_CASE("log_entry equality") {
    log_entry a{1, "x"};
    log_entry b{1, "x"};
    log_entry c{2, "x"};
    CHECK(a == b);
    CHECK(a != c);
}

TEST_CASE("message default fields") {
    message m;
    m.mtype = msg_type::request_vote_req;
    m.mterm = 1;
    m.msource = 1;
    m.mdest = 2;
    CHECK(m.mterm == 1);
    CHECK(m.msource == 1);
    CHECK_FALSE(m.mvote_granted.has_value());
    CHECK_FALSE(m.msuccess.has_value());
}

TEST_CASE("message equality") {
    message a;
    a.mtype = msg_type::request_vote_req;
    a.mterm = 1;
    a.msource = 1;
    a.mdest = 2;
    a.mlast_log_term = 0;
    a.mlast_log_index = 0;

    message b = a;
    CHECK(a == b);

    b.mterm = 2;
    CHECK(a != b);
}

TEST_CASE("server_state values") {
    CHECK(server_state::follower != server_state::candidate);
    CHECK(server_state::candidate != server_state::leader);
}
