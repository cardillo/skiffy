#include "doctest/doctest.h"

#include "skiffy.hpp"
#include "test_utils.h"

using skiffy::node_id;

TEST_CASE("server initial state matches TLA+ Init") {
    skiffy::memory_transport t;
    skiffy::detail::test_server s(s1, {s2, s3}, t);

    CHECK(s.id() == s1);
    CHECK(s.current_term() == 1);
    CHECK(s.state() == skiffy::detail::server_state::follower);
    CHECK(s.voted_for() == skiffy::nil_id);
    CHECK(s.log().empty());
    CHECK(s.commit_index() == 0);
    CHECK(s.votes_responded().empty());
    CHECK(s.votes_granted().empty());
    CHECK(s.next_index_for(s1) == 1);
    CHECK(s.next_index_for(s2) == 1);
    CHECK(s.next_index_for(s3) == 1);
    CHECK(s.match_index_for(s1) == 0);
    CHECK(s.match_index_for(s2) == 0);
    CHECK(s.match_index_for(s3) == 0);
}

TEST_CASE("last_term is 0 on empty log") {
    skiffy::memory_transport t;
    skiffy::detail::test_server s(s1, {s2, s3}, t);
    CHECK(s.last_term() == 0);
}

TEST_CASE("is_quorum for 3-node cluster") {
    skiffy::memory_transport t;
    skiffy::detail::test_server s(s1, {s2, s3}, t);

    CHECK_FALSE(s.is_quorum({}));
    CHECK_FALSE(s.is_quorum({s1}));
    CHECK(s.is_quorum({s1, s2}));
    CHECK(s.is_quorum({s1, s2, s3}));
}

TEST_CASE("is_quorum for 5-node cluster") {
    skiffy::memory_transport t;
    skiffy::detail::test_server s(s1, {s2, s3, s4, s5}, t);

    CHECK_FALSE(s.is_quorum({s1}));
    CHECK_FALSE(s.is_quorum({s1, s2}));
    CHECK(s.is_quorum({s1, s2, s3}));
    CHECK(s.is_quorum({s1, s2, s3, s4}));
}
