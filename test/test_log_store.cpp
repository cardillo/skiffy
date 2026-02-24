#include "doctest/doctest.h"
#include "raftpp.h"

#include <cstdio>

using namespace raftpp;

// -------------------------------------------------------
// memory_log_store
// -------------------------------------------------------

TEST_CASE("memory_log_store: basic operations") {
    memory_log_store s;
    CHECK(s.empty());
    CHECK(s.size() == 0);

    log_entry e1{1, entry_type::data, "a"};
    log_entry e2{2, entry_type::data, "b"};
    s.append(e1);
    s.append(e2);

    CHECK(s.size() == 2);
    CHECK(!s.empty());
    CHECK(s[0] == e1);
    CHECK(s[1] == e2);
    CHECK(s.back() == e2);
}

TEST_CASE("memory_log_store: truncate") {
    memory_log_store s;
    s.append({1, entry_type::data, "a"});
    s.append({2, entry_type::data, "b"});
    s.append({3, entry_type::data, "c"});

    s.truncate(1);
    CHECK(s.size() == 1);
    CHECK(s[0].value == "a");
}

TEST_CASE("memory_log_store: clear") {
    memory_log_store s;
    s.append({1, entry_type::data, "a"});
    s.clear();
    CHECK(s.empty());
}

TEST_CASE("memory_log_store: entries()") {
    memory_log_store s;
    s.append({1, entry_type::data, "x"});
    s.append({1, entry_type::data, "y"});

    auto& v = s.entries();
    REQUIRE(v.size() == 2);
    CHECK(v[0].value == "x");
    CHECK(v[1].value == "y");
}

// -------------------------------------------------------
// file_log_store
// -------------------------------------------------------

static std::string tmp_prefix() {
    return "/tmp/raftpp_test_store";
}

static void cleanup_files() {
    std::remove((tmp_prefix() + ".wal").c_str());
    std::remove((tmp_prefix() + ".snap").c_str());
}

TEST_CASE("file_log_store: append and load") {
    cleanup_files();
    {
        file_log_store s(tmp_prefix());
        s.append({1, entry_type::data, "x"});
        s.append({2, entry_type::data, "y"});
    }
    {
        file_log_store s(tmp_prefix());
        s.load();
        REQUIRE(s.size() == 2);
        CHECK(s[0].term  == 1);
        CHECK(s[0].value == "x");
        CHECK(s[1].term  == 2);
        CHECK(s[1].value == "y");
    }
    cleanup_files();
}

TEST_CASE("file_log_store: truncate and reload") {
    cleanup_files();
    {
        file_log_store s(tmp_prefix());
        s.append({1, entry_type::data, "a"});
        s.append({2, entry_type::data, "b"});
        s.append({3, entry_type::data, "c"});
        s.truncate(1);
        CHECK(s.size() == 1);
    }
    {
        file_log_store s(tmp_prefix());
        s.load();
        REQUIRE(s.size() == 1);
        CHECK(s[0].value == "a");
    }
    cleanup_files();
}

TEST_CASE("file_log_store: clear and reload") {
    cleanup_files();
    {
        file_log_store s(tmp_prefix());
        s.append({1, entry_type::data, "a"});
        s.clear();
    }
    {
        file_log_store s(tmp_prefix());
        s.load();
        CHECK(s.empty());
    }
    cleanup_files();
}

TEST_CASE("file_log_store: entry_type preserved") {
    cleanup_files();
    {
        file_log_store s(tmp_prefix());
        s.append(
            {1, entry_type::config_joint, "cfg"});
    }
    {
        file_log_store s(tmp_prefix());
        s.load();
        REQUIRE(s.size() == 1);
        CHECK(s[0].type == entry_type::config_joint);
        CHECK(s[0].value == "cfg");
    }
    cleanup_files();
}

TEST_CASE("file_log_store: snapshot save/load") {
    cleanup_files();
    snapshot_t snap;
    snap.index = 5;
    snap.term  = 2;
    snap.data  = "test-data";

    {
        file_log_store s(tmp_prefix());
        s.save_snapshot(snap);
    }
    {
        file_log_store s(tmp_prefix());
        auto loaded = s.load_snapshot();
        REQUIRE(loaded.has_value());
        CHECK(loaded->index == 5);
        CHECK(loaded->term  == 2);
        CHECK(loaded->data  == "test-data");
    }
    cleanup_files();
}

TEST_CASE("file_log_store: load_snapshot absent") {
    cleanup_files();
    file_log_store s(tmp_prefix());
    auto loaded = s.load_snapshot();
    CHECK(!loaded.has_value());
    cleanup_files();
}

TEST_CASE("log_entry roundtrip: 3-field codec") {
    log_entry e{3, entry_type::config_final,
                "peers"};
    message m;
    m.type   = msg_type::append_entries_req;
    m.term   = 1;
    m.from = 1;
    m.to   = 2;
    m.prev_log_index = 0;
    m.prev_log_term  = 0;
    m.entries        = {e};
    m.commit_index   = 0;

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, m);
    msgpack::object_handle oh =
        msgpack::unpack(sbuf.data(), sbuf.size());
    message m2;
    oh.get().convert(m2);

    REQUIRE(m2.entries.has_value());
    REQUIRE(m2.entries->size() == 1);
    CHECK(m2.entries->at(0).term  == 3);
    CHECK(m2.entries->at(0).type ==
          entry_type::config_final);
    CHECK(m2.entries->at(0).value == "peers");
}
