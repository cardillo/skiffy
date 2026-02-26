#include "doctest/doctest.h"

#include "raftpp.h"
#include "test_utils.h"

TEST_CASE("server initial state matches TLA+ Init") {
    raftpp::memory_transport t;
    raftpp::server s(1, {2, 3}, t);

    CHECK(s.id() == 1);
    CHECK(s.current_term() == 1);
    CHECK(s.state() == raftpp::server_state::follower);
    CHECK(s.voted_for() == raftpp::nil_id);
    CHECK(s.log().empty());
    CHECK(s.commit_index() == 0);
    CHECK(s.votes_responded().empty());
    CHECK(s.votes_granted().empty());
    CHECK(s.next_index_for(1) == 1);
    CHECK(s.next_index_for(2) == 1);
    CHECK(s.next_index_for(3) == 1);
    CHECK(s.match_index_for(1) == 0);
    CHECK(s.match_index_for(2) == 0);
    CHECK(s.match_index_for(3) == 0);
}

TEST_CASE("last_term is 0 on empty log") {
    raftpp::memory_transport t;
    raftpp::server s(1, {2, 3}, t);
    CHECK(s.last_term() == 0);
}

TEST_CASE("is_quorum for 3-node cluster") {
    raftpp::memory_transport t;
    raftpp::server s(1, {2, 3}, t);

    CHECK_FALSE(s.is_quorum({}));
    CHECK_FALSE(s.is_quorum({1}));
    CHECK(s.is_quorum({1, 2}));
    CHECK(s.is_quorum({1, 2, 3}));
}

TEST_CASE("is_quorum for 5-node cluster") {
    raftpp::memory_transport t;
    raftpp::server s(1, {2, 3, 4, 5}, t);

    CHECK_FALSE(s.is_quorum({1}));
    CHECK_FALSE(s.is_quorum({1, 2}));
    CHECK(s.is_quorum({1, 2, 3}));
    CHECK(s.is_quorum({1, 2, 3, 4}));
}
