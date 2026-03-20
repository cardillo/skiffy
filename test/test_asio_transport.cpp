#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include "doctest/doctest.h"

#include "skiffy.hpp"
#include "test_utils.h"

// -------------------------------------------------------
// Group 1 — pure unit (no io thread)
// -------------------------------------------------------

TEST_CASE("asio_transport send to unknown peer is silent") {
    skiffy::asio_transport t(s1);

    skiffy::message msg;
    msg.type = skiffy::msg_type::request_vote_req;
    msg.term = 1;
    msg.from = s1;
    msg.to = skiffy::node_id({10, 10, 10, 10}, 0);

    t.send(msg);
    CHECK(true);
}

TEST_CASE("asio_transport remove peer: subsequent send is silent") {
    skiffy::asio_transport t(s1);
    skiffy::node_id peer({127, 0, 0, 1}, 19999);

    // send lazily creates peer_conn; remove cleans it up
    skiffy::message msg;
    msg.type = skiffy::msg_type::request_vote_req;
    msg.term = 1;
    msg.from = s1;
    msg.to = peer;
    t.send(msg);
    t.remove_peer(peer);

    // further send to same addr recreates lazily
    t.send(msg);
    CHECK(true);
}

// -------------------------------------------------------
// Group 2 — loopback TCP integration
// -------------------------------------------------------

TEST_CASE("asio_transport delivers one message over loopback") {
    using namespace std::chrono_literals;

    skiffy::asio_transport tR(s2);
    tR.listen(0);
    uint16_t port = tR.bound_port();

    std::promise<skiffy::message> prom;
    auto fut = prom.get_future();
    bool fired = false;
    tR.on_message([&](const skiffy::message& m) {
        if (!fired) {
            fired = true;
            prom.set_value(m);
        }
    });
    tR.run();

    skiffy::asio_transport tS(s1);
    tS.run();

    skiffy::node_id peer_id({127, 0, 0, 1}, port);
    skiffy::message out;
    out.type = skiffy::msg_type::request_vote_req;
    out.term = 7;
    out.from = s1;
    out.to = peer_id;
    tS.send(out);

    auto st = fut.wait_for(3s);
    REQUIRE(st == std::future_status::ready);
    auto got = fut.get();
    CHECK(got.term == 7);
    CHECK(got.type == skiffy::msg_type::request_vote_req);

    tS.stop();
    tR.stop();
}

TEST_CASE("asio_transport delivers multiple messages in order") {
    using namespace std::chrono_literals;

    skiffy::asio_transport tR(s2);
    tR.listen(0);
    uint16_t port = tR.bound_port();

    std::promise<std::vector<skiffy::message>> prom;
    auto fut = prom.get_future();
    std::vector<skiffy::message> received;
    bool fired = false;
    tR.on_message([&](const skiffy::message& m) {
        received.push_back(m);
        if (!fired && received.size() == 3) {
            fired = true;
            prom.set_value(received);
        }
    });
    tR.run();

    skiffy::asio_transport tS(s1);
    skiffy::node_id peer_id({127, 0, 0, 1}, port);
    tS.run();

    for (skiffy::term_t term = 1; term <= 3; ++term) {
        skiffy::message msg;
        msg.type = skiffy::msg_type::append_entries_req;
        msg.term = term;
        msg.from = s1;
        msg.to = peer_id;
        tS.send(msg);
    }

    auto st = fut.wait_for(3s);
    REQUIRE(st == std::future_status::ready);
    auto msgs = fut.get();
    REQUIRE(msgs.size() == 3);
    CHECK(msgs[0].term == 1);
    CHECK(msgs[1].term == 2);
    CHECK(msgs[2].term == 3);

    std::this_thread::sleep_for(50ms);
    tS.stop();
    tR.stop();
}

TEST_CASE("asio_transport all message fields preserved end-to-end") {
    using namespace std::chrono_literals;

    skiffy::asio_transport tR(s2);
    tR.listen(0);
    uint16_t port = tR.bound_port();

    std::promise<skiffy::message> prom;
    auto fut = prom.get_future();
    bool fired = false;
    tR.on_message([&](const skiffy::message& m) {
        if (!fired) {
            fired = true;
            prom.set_value(m);
        }
    });
    tR.run();

    skiffy::asio_transport tS(s1);
    skiffy::node_id peer_id({127, 0, 0, 1}, port);
    tS.run();

    skiffy::message out;
    out.type = skiffy::msg_type::request_vote_resp;
    out.term = 4;
    out.from = s1;
    out.to = peer_id;
    out.vote_granted = true;
    out.last_log_index = 5;
    out.last_log_term = 3;
    tS.send(out);

    auto st = fut.wait_for(3s);
    REQUIRE(st == std::future_status::ready);
    auto got = fut.get();
    CHECK(got.type == skiffy::msg_type::request_vote_resp);
    CHECK(got.term == 4);
    CHECK(got.from == s1);
    CHECK(got.to == peer_id);
    CHECK(got.vote_granted == true);
    CHECK(got.last_log_index == 5);
    CHECK(got.last_log_term == 3);

    tS.stop();
    tR.stop();
}

TEST_CASE("asio_transport bidirectional exchange") {
    using namespace std::chrono_literals;

    skiffy::asio_transport tA(s1);
    skiffy::asio_transport tB(s2);
    tA.listen(0);
    tB.listen(0);
    uint16_t portA = tA.bound_port();
    uint16_t portB = tB.bound_port();

    std::promise<skiffy::message> promB;
    auto futB = promB.get_future();
    bool firedB = false;
    tB.on_message([&](const skiffy::message& m) {
        if (!firedB) {
            firedB = true;
            promB.set_value(m);
        }
    });

    std::promise<skiffy::message> promA;
    auto futA = promA.get_future();
    bool firedA = false;
    tA.on_message([&](const skiffy::message& m) {
        if (!firedA) {
            firedA = true;
            promA.set_value(m);
        }
    });

    tA.run();
    tB.run();

    skiffy::message req;
    req.type = skiffy::msg_type::request_vote_req;
    req.term = 10;
    req.from = s1;
    req.to = skiffy::node_id({127, 0, 0, 1}, portB);
    tA.send(req);

    skiffy::message resp;
    resp.type = skiffy::msg_type::request_vote_resp;
    resp.term = 10;
    resp.from = s2;
    resp.to = skiffy::node_id({127, 0, 0, 1}, portA);
    resp.vote_granted = true;
    tB.send(resp);

    auto stB = futB.wait_for(3s);
    REQUIRE(stB == std::future_status::ready);
    auto gotB = futB.get();
    CHECK(gotB.term == 10);
    CHECK(gotB.type == skiffy::msg_type::request_vote_req);

    auto stA = futA.wait_for(3s);
    REQUIRE(stA == std::future_status::ready);
    auto gotA = futA.get();
    CHECK(gotA.term == 10);
    CHECK(gotA.type == skiffy::msg_type::request_vote_resp);
    CHECK(gotA.vote_granted == true);

    tA.stop();
    tB.stop();
}

// -------------------------------------------------------
// Group 3 — error path coverage
// -------------------------------------------------------

TEST_CASE("asio_transport: connection failure is silent") {
    using namespace std::chrono_literals;
    skiffy::asio_transport t(s1);

    // use port 1 — connection refused on loopback
    skiffy::node_id closed_id({127, 0, 0, 1}, 1);
    skiffy::message msg;
    msg.type = skiffy::msg_type::request_vote_req;
    msg.term = 1;
    msg.from = s1;
    msg.to = closed_id;
    t.send(msg);

    t.run();
    std::this_thread::sleep_for(500ms);
    t.stop();
    CHECK(true);
}

TEST_CASE("asio_transport: read error on peer disconnect") {
    using namespace std::chrono_literals;
    skiffy::asio_transport t(s2);
    t.listen(0);
    uint16_t port = t.bound_port();
    t.run();

    // raw client: connect and immediately close
    {
        asio::io_context tmp;
        asio::ip::tcp::socket s(tmp);
        asio::error_code ec;
        s.connect({asio::ip::make_address("127.0.0.1"), port}, ec);
    } // socket destructs -> EOF on receiver's do_read_loop

    std::this_thread::sleep_for(500ms);
    t.stop();
    CHECK(true);
}
