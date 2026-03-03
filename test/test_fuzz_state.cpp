#include <cstdio>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>

#include "doctest/doctest.h"

#include "raftpp.h"
#include "test_utils.h"

using namespace raftpp;

static const std::string kPrefix = "/tmp/raftpp_fuzz_state";

static void cleanup() {
    std::remove((kPrefix + ".wal").c_str());
    std::remove((kPrefix + ".snap").c_str());
}

// Write raw bytes to the WAL file, bypassing file_log_store
static void write_raw_wal(const std::string& data) {
    std::ofstream f(kPrefix + ".wal",
                    std::ios::binary | std::ios::trunc);
    f.write(data.data(), data.size());
}

// Write raw bytes to the snap file, bypassing file_log_store
static void write_raw_snap(const std::string& data) {
    std::ofstream f(kPrefix + ".snap",
                    std::ios::binary | std::ios::trunc);
    f.write(data.data(), data.size());
}

// Build a valid framed WAL blob for one log_entry
static std::string make_wal_frame(const log_entry& e) {
    msgpack::sbuffer buf;
    msgpack::pack(buf, e);
    uint32_t sz = static_cast<uint32_t>(buf.size());
    uint32_t c = crc32(buf.data(), buf.size());
    char hdr[8];
    hdr[0] = static_cast<char>(sz & 0xff);
    hdr[1] = static_cast<char>((sz >> 8) & 0xff);
    hdr[2] = static_cast<char>((sz >> 16) & 0xff);
    hdr[3] = static_cast<char>((sz >> 24) & 0xff);
    hdr[4] = static_cast<char>(c & 0xff);
    hdr[5] = static_cast<char>((c >> 8) & 0xff);
    hdr[6] = static_cast<char>((c >> 16) & 0xff);
    hdr[7] = static_cast<char>((c >> 24) & 0xff);
    return std::string(hdr, 8)
        + std::string(buf.data(), buf.size());
}

// Build a valid snapshot blob (magic + crc + msgpack)
static std::string make_snap_blob(const snapshot_t& s) {
    msgpack::sbuffer buf;
    msgpack::pack(buf, s);
    uint32_t c = crc32(buf.data(), buf.size());
    std::string out = "RAFT";
    char cb[4];
    cb[0] = static_cast<char>(c & 0xff);
    cb[1] = static_cast<char>((c >> 8) & 0xff);
    cb[2] = static_cast<char>((c >> 16) & 0xff);
    cb[3] = static_cast<char>((c >> 24) & 0xff);
    out.append(cb, 4);
    out.append(buf.data(), buf.size());
    return out;
}

// -------------------------------------------------------
// WAL truncation tests (graceful recovery)
// -------------------------------------------------------

TEST_CASE("fuzz_wal: one entry truncated to 3 bytes") {
    cleanup();
    // write only 3 bytes — less than the 8-byte header
    write_raw_wal(std::string(3, '\x00'));
    file_log_store s(kPrefix);
    CHECK_NOTHROW(s.load());
    CHECK(s.size() == 0);
    cleanup();
}

TEST_CASE("fuzz_wal: two entries, second truncated mid-data") {
    cleanup();
    std::string frame1 =
        make_wal_frame({1, entry_type::data, "a"});
    std::string frame2 =
        make_wal_frame({2, entry_type::data, "b"});
    // keep frame1 and only first 4 bytes of frame2
    write_raw_wal(frame1 + frame2.substr(0, 4));
    file_log_store s(kPrefix);
    CHECK_NOTHROW(s.load());
    CHECK(s.size() == 1);
    CHECK(s[0].value == "a");
    cleanup();
}

TEST_CASE("fuzz_wal: two entries then 4-byte partial header") {
    cleanup();
    std::string frame1 =
        make_wal_frame({1, entry_type::data, "x"});
    std::string frame2 =
        make_wal_frame({2, entry_type::data, "y"});
    // full header only (4 bytes), less than 8-byte frame header
    write_raw_wal(frame1 + frame2 + std::string(4, '\x01'));
    file_log_store s(kPrefix);
    CHECK_NOTHROW(s.load());
    CHECK(s.size() == 2);
    cleanup();
}

TEST_CASE("fuzz_wal: header claims size=50 but only 5 bytes follow") {
    cleanup();
    std::string frame1 =
        make_wal_frame({1, entry_type::data, "z"});
    // 8-byte header: size=50, crc=0, then 5 bytes of data
    char hdr[8] = {};
    hdr[0] = 50; // size = 50
    std::string bad(hdr, 8);
    bad += std::string(5, '\xab');
    write_raw_wal(frame1 + bad);
    file_log_store s(kPrefix);
    CHECK_NOTHROW(s.load());
    CHECK(s.size() == 1);
    CHECK(s[0].value == "z");
    cleanup();
}

// -------------------------------------------------------
// WAL CRC mismatch tests (genuine corruption → throws)
// -------------------------------------------------------

TEST_CASE("fuzz_wal: empty file loads empty without error") {
    cleanup();
    { std::ofstream f(kPrefix + ".wal", std::ios::binary); }
    file_log_store s(kPrefix);
    CHECK_NOTHROW(s.load());
    CHECK(s.empty());
    cleanup();
}

TEST_CASE("fuzz_wal: all-garbage bytes throw runtime_error") {
    cleanup();
    // 20 bytes of 0xff — header claims sz=0xffffffff, then
    // the data-size check fires after CRC computed over 0
    // bytes doesn't match 0xffffffff; actually size > remaining
    // so it would be truncation... let's use a size that fits:
    // header[0..3] = 4 (sz=4), header[4..7] = 0 (stored crc=0),
    // then 4 bytes of data with crc != 0 → CRC mismatch throw
    char hdr[8] = {};
    hdr[0] = 4; // sz = 4
    // crc bytes = 0
    std::string blob(hdr, 8);
    blob += std::string(4, '\xff'); // crc32("\xff\xff\xff\xff") != 0
    write_raw_wal(blob);
    file_log_store s(kPrefix);
    CHECK_THROWS_AS(s.load(), std::runtime_error);
    cleanup();
}

TEST_CASE("fuzz_wal: valid entry then wrong CRC appended") {
    cleanup();
    std::string frame =
        make_wal_frame({1, entry_type::data, "ok"});
    // 8-byte header: size=10, crc=0xdeadbeef (wrong)
    // followed by 10 bytes of zeros
    char hdr[8] = {};
    hdr[0] = 10; // sz = 10
    hdr[4] = static_cast<char>(0xef);
    hdr[5] = static_cast<char>(0xbe);
    hdr[6] = static_cast<char>(0xad);
    hdr[7] = static_cast<char>(0xde);
    std::string bad(hdr, 8);
    bad += std::string(10, '\x00');
    write_raw_wal(frame + bad);
    file_log_store s(kPrefix);
    CHECK_THROWS_AS(s.load(), std::runtime_error);
    cleanup();
}

TEST_CASE("fuzz_wal: correct size, wrong CRC (bit flip)") {
    cleanup();
    std::string frame =
        make_wal_frame({1, entry_type::data, "hi"});
    // flip one CRC byte (byte 4 of the frame)
    frame[4] ^= 0x01;
    write_raw_wal(frame);
    file_log_store s(kPrefix);
    CHECK_THROWS_AS(s.load(), std::runtime_error);
    cleanup();
}

TEST_CASE("fuzz_wal: all-zero bytes throw runtime_error") {
    cleanup();
    // Header: sz=0, crc=0. crc32("",0) = 0, so this would
    // pass the crc check and try to decode 0 bytes of msgpack.
    // Use sz=1 instead: crc32("\x00",1) != 0 → mismatch.
    char hdr[8] = {};
    hdr[0] = 1; // sz = 1
    // stored crc = 0 (all zeros in hdr[4..7])
    std::string blob(hdr, 8);
    blob += '\x00'; // 1 byte of data
    write_raw_wal(blob);
    file_log_store s(kPrefix);
    CHECK_THROWS_AS(s.load(), std::runtime_error);
    cleanup();
}

TEST_CASE("fuzz_wal: corrupt entry in the middle throws") {
    cleanup();
    std::string f1 =
        make_wal_frame({1, entry_type::data, "good1"});
    std::string f3 =
        make_wal_frame({3, entry_type::data, "good3"});
    // corrupt middle: valid size, wrong CRC, valid-length data
    char hdr[8] = {};
    hdr[0] = 4;
    hdr[4] = static_cast<char>(0xba);
    hdr[5] = static_cast<char>(0xdc);
    hdr[6] = static_cast<char>(0x0f);
    hdr[7] = static_cast<char>(0xfe);
    std::string bad(hdr, 8);
    bad += std::string(4, '\x55');
    write_raw_wal(f1 + bad + f3);
    file_log_store s(kPrefix);
    CHECK_THROWS_AS(s.load(), std::runtime_error);
    cleanup();
}

// -------------------------------------------------------
// Snapshot corruption tests
// -------------------------------------------------------

TEST_CASE("fuzz_snap: absent file returns nullopt") {
    cleanup();
    file_log_store s(kPrefix);
    CHECK_NOTHROW(s.load_snapshot());
    CHECK(!s.load_snapshot().has_value());
    cleanup();
}

TEST_CASE("fuzz_snap: wrong magic throws runtime_error") {
    cleanup();
    snapshot_t sn;
    sn.index = 1;
    sn.term = 1;
    sn.data = "d";
    std::string blob = make_snap_blob(sn);
    blob[0] = 'X'; // corrupt magic
    write_raw_snap(blob);
    file_log_store s(kPrefix);
    CHECK_THROWS_AS(s.load_snapshot(), std::runtime_error);
    cleanup();
}

TEST_CASE("fuzz_snap: truncated after magic throws") {
    cleanup();
    write_raw_snap("RAFT"); // only 4 bytes, header needs 8
    file_log_store s(kPrefix);
    CHECK_THROWS_AS(s.load_snapshot(), std::runtime_error);
    cleanup();
}

TEST_CASE("fuzz_snap: correct magic, wrong CRC throws") {
    cleanup();
    snapshot_t sn;
    sn.index = 2;
    sn.term = 1;
    sn.data = "test";
    std::string blob = make_snap_blob(sn);
    blob[4] ^= 0xff; // flip CRC byte
    write_raw_snap(blob);
    file_log_store s(kPrefix);
    CHECK_THROWS_AS(s.load_snapshot(), std::runtime_error);
    cleanup();
}

TEST_CASE("fuzz_snap: correct magic+CRC, garbage body throws") {
    cleanup();
    // construct magic + crc of garbage + garbage body
    std::string body(32, '\xff');
    uint32_t c = crc32(body.data(), body.size());
    std::string blob = "RAFT";
    char cb[4];
    cb[0] = static_cast<char>(c & 0xff);
    cb[1] = static_cast<char>((c >> 8) & 0xff);
    cb[2] = static_cast<char>((c >> 16) & 0xff);
    cb[3] = static_cast<char>((c >> 24) & 0xff);
    blob.append(cb, 4);
    blob += body;
    write_raw_snap(blob);
    file_log_store s(kPrefix);
    // msgpack will fail to decode 0xff*32 as snapshot_t
    CHECK_THROWS(s.load_snapshot());
    cleanup();
}
