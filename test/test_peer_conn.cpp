#include "doctest/doctest.h"

#include "skiffy.hpp"
#include "test_utils.h"

// Test the peer_conn_t state machine in isolation.
//
// Strategy: construct a peer_conn with an io_context that is never
// run.  Async operations (connect, write) are posted to the context
// but their callbacks never fire, so we drive state transitions with
// synthetic events (evt_ok, evt_fail, evt_err) and verify queue_,
// writing_, sock_, and the SML state directly.

using namespace skiffy;
namespace sml = boost::sml;

using PC = asio_transport::peer_conn;

static auto make_buf() {
    return std::make_shared<std::vector<uint8_t>>(std::vector<uint8_t>{0});
}

static asio::ip::tcp::endpoint test_ep() {
    // Port 19999 on loopback; listener absent so connect fails,
    // but the failure is only posted to io — never executed.
    return {asio::ip::make_address("127.0.0.1"), 19999};
}

// -------------------------------------------------------
// helpers
// -------------------------------------------------------

static bool in_disc(PC& pc) { return pc.sm_.is(sml::state<PC::s_disc>); }
static bool in_conn(PC& pc) { return pc.sm_.is(sml::state<PC::s_conn>); }
static bool in_live(PC& pc) { return pc.sm_.is(sml::state<PC::s_live>); }

// -------------------------------------------------------
// tests
// -------------------------------------------------------

TEST_CASE("peer_conn initial state is disc") {
    asio::io_context io;
    PC pc(s1, test_ep(), io);

    CHECK(in_disc(pc));
    CHECK(pc.queue_.empty());
    CHECK_FALSE(pc.writing_);
    CHECK_FALSE(pc.sock_);
}

TEST_CASE("peer_conn send in disc queues and starts connect") {
    asio::io_context io;
    PC pc(s1, test_ep(), io);

    pc.send(make_buf());

    CHECK(in_conn(pc));
    CHECK(pc.queue_.size() == 1);
    CHECK(pc.sock_); // start_connect created the socket
}

TEST_CASE("peer_conn send in conn only queues") {
    asio::io_context io;
    PC pc(s1, test_ep(), io);

    pc.send(make_buf()); // disc -> conn
    pc.send(make_buf()); // stays conn
    pc.send(make_buf()); // stays conn

    CHECK(in_conn(pc));
    CHECK(pc.queue_.size() == 3);
}

TEST_CASE("peer_conn evt_ok flushes queue into live") {
    asio::io_context io;
    PC pc(s1, test_ep(), io);

    pc.send(make_buf());
    pc.send(make_buf());
    CHECK(pc.queue_.size() == 2);

    pc.sm_.process_event(PC::evt_ok{});

    CHECK(in_live(pc));
    CHECK(pc.writing_);
    // do_write_next() pops the front buf immediately; 1 remains
    // (async_write posted to io, never executed here)
    CHECK(pc.queue_.size() == 1);
}

TEST_CASE("peer_conn evt_fail clears queue back to disc") {
    asio::io_context io;
    PC pc(s1, test_ep(), io);

    pc.send(make_buf());
    pc.send(make_buf());

    pc.sm_.process_event(PC::evt_fail{});

    CHECK(in_disc(pc));
    CHECK(pc.queue_.empty());
    CHECK_FALSE(pc.sock_);
}

TEST_CASE("peer_conn send in live while writing only queues") {
    asio::io_context io;
    PC pc(s1, test_ep(), io);

    pc.send(make_buf());
    pc.sm_.process_event(PC::evt_ok{}); // -> live, writing_ = true

    pc.send(make_buf());
    pc.send(make_buf());

    // writing_ stays true; no extra write started.
    // do_write_next() already popped the original buf;
    // 2 new sends queued = 2
    CHECK(in_live(pc));
    CHECK(pc.writing_);
    CHECK(pc.queue_.size() == 2);
}

TEST_CASE("peer_conn send in live while idle starts write") {
    asio::io_context io;
    PC pc(s1, test_ep(), io);

    pc.send(make_buf());
    pc.sm_.process_event(PC::evt_ok{}); // -> live, do_write_next pops buf

    // buf already dequeued by do_write_next; async_write pending
    CHECK(pc.queue_.empty());
    CHECK(pc.writing_);

    // simulate write completing
    pc.writing_ = false;

    pc.send(make_buf()); // should start a new write immediately

    CHECK(in_live(pc));
    CHECK(pc.writing_);
    // do_write_next popped the buf immediately; queue empty
    CHECK(pc.queue_.empty());
}

TEST_CASE("peer_conn evt_err returns to disc, retains queue") {
    asio::io_context io;
    PC pc(s1, test_ep(), io);

    pc.send(make_buf());
    pc.send(make_buf());
    pc.sm_.process_event(PC::evt_ok{}); // -> live, do_write_next pops 1

    // 1 buf dequeued immediately; 1 remains
    // simulate write error
    pc.sm_.process_event(PC::evt_err{});

    CHECK(in_disc(pc));
    CHECK_FALSE(pc.writing_);
    CHECK_FALSE(pc.sock_);
    // 1 buffer retained for retry
    CHECK(pc.queue_.size() == 1);
}

TEST_CASE("peer_conn reconnects after fail") {
    asio::io_context io;
    PC pc(s1, test_ep(), io);

    pc.send(make_buf());
    pc.sm_.process_event(PC::evt_fail{}); // back to disc, empty

    pc.send(make_buf()); // disc -> conn, new socket

    CHECK(in_conn(pc));
    CHECK(pc.queue_.size() == 1);
    CHECK(pc.sock_);
}
