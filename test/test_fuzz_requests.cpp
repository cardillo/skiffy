#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "doctest/doctest.h"

#include "raftpp.h"
#include "test_utils.h"

using namespace raftpp;

// -------------------------------------------------------
// Invalid msg_type
// -------------------------------------------------------

TEST_CASE("fuzz_req: msg_type=7 (past valid) handled") {
    memory_transport t;
    server<memory_transport> srv(s1, {s2, s3}, t);
    message m;
    m.type = static_cast<msg_type>(7);
    m.term = 0;
    m.from = s2;
    m.to = s1;
    CHECK_NOTHROW(srv.receive(m));
}

TEST_CASE("fuzz_req: msg_type=255 handled") {
    memory_transport t;
    server<memory_transport> srv(s1, {s2, s3}, t);
    message m;
    m.type = static_cast<msg_type>(255);
    m.term = 0;
    m.from = s2;
    m.to = s1;
    CHECK_NOTHROW(srv.receive(m));
}

// -------------------------------------------------------
// All-nullopt field messages
// -------------------------------------------------------

TEST_CASE("fuzz_req: append_entries_req all-nullopt") {
    memory_transport t;
    server<memory_transport> srv(s1, {s2, s3}, t);
    message m;
    m.type = msg_type::append_entries_req;
    m.term = 0;
    m.from = s2;
    m.to = s1;
    // all optionals absent: prev_log_index/term=0, entries=empty,
    // commit_index=0 → accepted (prev=0→log_ok), no entries,
    // apply_committed loops 0 times (last_applied=0,
    // commit=0: idx=1>0 fails)
    CHECK_NOTHROW(srv.receive(m));
}

TEST_CASE("fuzz_req: request_vote_req no last_log fields") {
    memory_transport t;
    server<memory_transport> srv(s1, {s2, s3}, t);
    message m;
    m.type = msg_type::request_vote_req;
    m.term = 0;
    m.from = s2;
    m.to = s1;
    // last_log_term/index absent → value_or(0) used
    CHECK_NOTHROW(srv.receive(m));
}

TEST_CASE("fuzz_req: install_snapshot_req all-nullopt") {
    memory_transport t;
    server<memory_transport> srv(s1, {s2, s3}, t);
    message m;
    m.type = msg_type::install_snapshot_req;
    m.term = 0;
    m.from = s2;
    m.to = s1;
    // si=0, snapshot_index_=0: 0>=0 → skipped immediately
    CHECK_NOTHROW(srv.receive(m));
}

TEST_CASE("fuzz_req: append_entries_resp all-nullopt") {
    memory_transport t;
    server<memory_transport> srv(s1, {s2, s3}, t);
    message m;
    m.type = msg_type::append_entries_resp;
    m.term = 0;
    m.from = s2;
    m.to = s1;
    // follower ignores ae_resp (SML) or term mismatch
    CHECK_NOTHROW(srv.receive(m));
}

// -------------------------------------------------------
// Edge-case values
// -------------------------------------------------------

TEST_CASE("fuzz_req: term=UINT64_MAX triggers higher_term") {
    memory_transport t;
    server<memory_transport> srv(s1, {s2, s3}, t);
    message m;
    m.type = msg_type::request_vote_req;
    m.term = std::numeric_limits<uint64_t>::max();
    m.from = s2;
    m.to = s1;
    // higher_term handler sets current_term=UINT64_MAX,
    // transitions to follower; no crash
    CHECK_NOTHROW(srv.receive(m));
}

TEST_CASE("fuzz_req: commit_index=UINT64_MAX, rejected AE") {
    memory_transport t;
    server<memory_transport> srv(s1, {s2, s3}, t);
    // prev_log_index=1 on empty log → log_ok fails → rejection
    // apply_committed() is not called despite huge commit_index
    message m;
    m.type = msg_type::append_entries_req;
    m.term = 0;
    m.from = s2;
    m.to = s1;
    m.prev_log_index = 1; // empty log → log_ok false
    m.commit_index = std::numeric_limits<uint64_t>::max();
    CHECK_NOTHROW(srv.receive(m));
    // server must have sent a negative AE response
    REQUIRE(!t.sent.empty());
    CHECK(!t.sent.back().success.value_or(true));
}

TEST_CASE("fuzz_req: prev_log_index=UINT64_MAX → rejection") {
    memory_transport t;
    server<memory_transport> srv(s1, {s2, s3}, t);
    message m;
    m.type = msg_type::append_entries_req;
    m.term = 0;
    m.from = s2;
    m.to = s1;
    m.prev_log_index = std::numeric_limits<uint64_t>::max();
    m.commit_index = 0;
    // log_ok: UINT64_MAX != 0, UINT64_MAX > last_log_index(0) → fail
    CHECK_NOTHROW(srv.receive(m));
    REQUIRE(!t.sent.empty());
    CHECK(!t.sent.back().success.value_or(true));
}

TEST_CASE("fuzz_req: snapshot_index=UINT64_MAX, caught") {
    memory_transport t;
    server<memory_transport> srv(s1, {s2, s3}, t);
    // snapshot_index_=0 < UINT64_MAX → attempts install("")
    // msgpack::unpack("",0) throws; caught by receive() guard
    message m;
    m.type = msg_type::install_snapshot_req;
    m.term = 0;
    m.from = s2;
    m.to = s1;
    m.snapshot_index = std::numeric_limits<uint64_t>::max();
    CHECK_NOTHROW(srv.receive(m));
}

TEST_CASE("fuzz_req: entries with unknown entry_type(99)") {
    memory_transport t;
    server<memory_transport> srv(s1, {s2, s3}, t);
    // send AE req with entries of invalid type; appended fine,
    // apply_committed skips unknown type silently
    message m;
    m.type = msg_type::append_entries_req;
    m.term = 0;
    m.from = s2;
    m.to = s1;
    m.prev_log_index = 0;
    m.commit_index = 1;
    m.entries = std::vector<log_entry>{
        {1, static_cast<entry_type>(99), "x"}};
    CHECK_NOTHROW(srv.receive(m));
}

// -------------------------------------------------------
// Deserialization bounds check (msgpack_unpack guard)
// -------------------------------------------------------

TEST_CASE("fuzz_req: empty array → msgpack type_error") {
    msgpack::sbuffer buf;
    msgpack::packer<msgpack::sbuffer> pk(buf);
    pk.pack_array(0);
    msgpack::object_handle oh =
        msgpack::unpack(buf.data(), buf.size());
    message m;
    CHECK_THROWS_AS(oh.get().convert(m), msgpack::type_error);
}

TEST_CASE("fuzz_req: 1-element array → msgpack type_error") {
    msgpack::sbuffer buf;
    msgpack::packer<msgpack::sbuffer> pk(buf);
    pk.pack_array(1);
    pk.pack(static_cast<uint8_t>(0));
    msgpack::object_handle oh =
        msgpack::unpack(buf.data(), buf.size());
    message m;
    CHECK_THROWS_AS(oh.get().convert(m), msgpack::type_error);
}

TEST_CASE("fuzz_req: non-array msgpack object → type_error") {
    // pack an integer (not array) and try to convert to message
    msgpack::sbuffer buf;
    msgpack::pack(buf, 42);
    msgpack::object_handle oh =
        msgpack::unpack(buf.data(), buf.size());
    message m;
    CHECK_THROWS_AS(oh.get().convert(m), msgpack::type_error);
}

// -------------------------------------------------------
// Wrong recipient
// -------------------------------------------------------

TEST_CASE("fuzz_req: wrong recipient dropped, no crash") {
    memory_transport t;
    server<memory_transport> srv(s1, {s2, s3}, t);
    message m;
    m.type = msg_type::append_entries_req;
    m.term = 0;
    m.from = s3;
    m.to = s2; // s2 != s1
    // server drops it (to != id_), no side effects
    CHECK_NOTHROW(srv.receive(m));
    CHECK(t.sent.empty());
}
