#include "doctest/doctest.h"

#include "skiffy.hpp"
#include "test_utils.h"

using namespace skiffy;

TEST_CASE("encode/decode roundtrip: request_vote_req") {
    message m;
    m.type = msg_type::request_vote_req;
    m.term = 3;
    m.from = s1;
    m.to = s2;
    m.last_log_term = 2;
    m.last_log_index = 5;

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, m);
    msgpack::object_handle oh = msgpack::unpack(sbuf.data(), sbuf.size());
    message m2;
    oh.get().convert(m2);

    CHECK(m2 == m);
}

TEST_CASE("encode/decode roundtrip: request_vote_resp") {
    message m;
    m.type = msg_type::request_vote_resp;
    m.term = 3;
    m.from = s2;
    m.to = s1;
    m.vote_granted = true;

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, m);
    msgpack::object_handle oh = msgpack::unpack(sbuf.data(), sbuf.size());
    message m2;
    oh.get().convert(m2);

    CHECK(m2 == m);
}

TEST_CASE("encode/decode roundtrip: append_entries_req") {
    message m;
    m.type = msg_type::append_entries_req;
    m.term = 4;
    m.from = s1;
    m.to = s2;
    m.prev_log_index = 2;
    m.prev_log_term = 3;
    m.entries = {log_entry{4, entry_type::data, "hello"}};
    m.commit_index = 2;

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, m);
    msgpack::object_handle oh = msgpack::unpack(sbuf.data(), sbuf.size());
    message m2;
    oh.get().convert(m2);

    CHECK(m2 == m);
}

TEST_CASE("encode/decode roundtrip: append_entries_resp") {
    message m;
    m.type = msg_type::append_entries_resp;
    m.term = 4;
    m.from = s2;
    m.to = s1;
    m.success = true;
    m.match_index = 3;

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, m);
    msgpack::object_handle oh = msgpack::unpack(sbuf.data(), sbuf.size());
    message m2;
    oh.get().convert(m2);

    CHECK(m2 == m);
}

TEST_CASE("absent optionals survive roundtrip") {
    message m;
    m.type = msg_type::append_entries_resp;
    m.term = 1;
    m.from = s2;
    m.to = s1;
    m.success = false;
    m.match_index = 0;

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, m);
    msgpack::object_handle oh = msgpack::unpack(sbuf.data(), sbuf.size());
    message m2;
    oh.get().convert(m2);

    CHECK(m2 == m);
    CHECK(!m2.last_log_term.has_value());
    CHECK(!m2.entries.has_value());
}

TEST_CASE("log_entry with empty entries vector") {
    message m;
    m.type = msg_type::append_entries_req;
    m.term = 1;
    m.from = s1;
    m.to = s2;
    m.prev_log_index = 0;
    m.prev_log_term = 0;
    m.entries = std::vector<log_entry>{};
    m.commit_index = 0;

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, m);
    msgpack::object_handle oh = msgpack::unpack(sbuf.data(), sbuf.size());
    message m2;
    oh.get().convert(m2);

    CHECK(m2 == m);
    REQUIRE(m2.entries.has_value());
    CHECK(m2.entries->empty());
}
