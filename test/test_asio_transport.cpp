#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include "doctest/doctest.h"

#include "skiffy.h"
#include "test_utils.h"

// -------------------------------------------------------
// helpers
// -------------------------------------------------------

static void run_acceptor(asio::io_context& io, asio::ip::tcp::acceptor& acc,
                         skiffy::asio_transport& t) {
    auto sock = std::make_shared<asio::ip::tcp::socket>(io);
    acc.async_accept(*sock, [&io, &acc, &t, sock](asio::error_code ec) {
        if (!ec) {
            auto tag = std::make_shared<std::array<uint8_t, 1>>();
            asio::async_read(
                *sock, asio::buffer(*tag),
                [&t, sock, tag](asio::error_code e2, size_t) {
                    if (e2)
                        return;
                    if (static_cast<skiffy::protocol_tag>((*tag)[0]) ==
                        skiffy::protocol_tag::raft)
                        t.accept_connection(sock);
                });
        }
        run_acceptor(io, acc, t);
    });
}

static asio::ip::tcp::acceptor make_acceptor(asio::io_context& io) {
    using tcp = asio::ip::tcp;
    tcp::acceptor acc(io);
    acc.open(tcp::v4());
    acc.set_option(tcp::acceptor::reuse_address(true));
    acc.bind({asio::ip::make_address("127.0.0.1"), 0});
    acc.listen();
    return acc;
}

// -------------------------------------------------------
// Group 1 — pure unit (no io thread)
// -------------------------------------------------------

TEST_CASE("asio_transport send to unknown peer is silent") {
    asio::io_context io;
    skiffy::asio_transport t(s1, io);

    skiffy::message msg;
    msg.type = skiffy::msg_type::request_vote_req;
    msg.term = 1;
    msg.from = s1;
    msg.to = skiffy::server_id({10, 10, 10, 10}, 0);

    t.send(msg);
    CHECK(true);
}

TEST_CASE("asio_transport remove peer: subsequent send is silent") {
    asio::io_context io;
    skiffy::asio_transport t(s1, io);

    t.add_peer(skiffy::server_id({127, 0, 0, 1}, 19999));
    t.remove_peer(skiffy::server_id({127, 0, 0, 1}, 19999));

    skiffy::message msg;
    msg.type = skiffy::msg_type::request_vote_req;
    msg.term = 1;
    msg.from = s1;
    msg.to = s2;

    t.send(msg);
    CHECK(true);
}

// -------------------------------------------------------
// Group 2 — loopback TCP integration
// -------------------------------------------------------

TEST_CASE("asio_transport delivers one message over loopback") {
    using namespace std::chrono_literals;
    asio::io_context io;

    skiffy::asio_transport tR(s2, io);
    auto acc = make_acceptor(io);
    uint16_t port = acc.local_endpoint().port();
    run_acceptor(io, acc, tR);

    std::promise<skiffy::message> prom;
    auto fut = prom.get_future();
    bool fired = false;
    tR.on_message([&](const skiffy::message& m) {
        if (!fired) {
            fired = true;
            prom.set_value(m);
        }
    });

    skiffy::asio_transport tS(s1, io);
    tS.add_peer(skiffy::server_id({127, 0, 0, 1}, port));

    std::thread th([&] { io.run_for(5s); });

    skiffy::server_id peer_id({127, 0, 0, 1}, port);
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

    io.stop();
    th.join();
}

TEST_CASE("asio_transport delivers multiple messages in order") {
    using namespace std::chrono_literals;
    asio::io_context io;

    skiffy::asio_transport tR(s2, io);
    auto acc = make_acceptor(io);
    uint16_t port = acc.local_endpoint().port();
    run_acceptor(io, acc, tR);

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

    skiffy::asio_transport tS(s1, io);
    skiffy::server_id peer_id({127, 0, 0, 1}, port);
    tS.add_peer(peer_id);

    std::thread th([&] { io.run_for(5s); });

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

    // let sender write callbacks drain so do_write_next
    // hits the queue_.empty() branch
    std::this_thread::sleep_for(50ms);
    io.stop();
    th.join();
}

TEST_CASE("asio_transport all message fields preserved end-to-end") {
    using namespace std::chrono_literals;
    asio::io_context io;

    skiffy::asio_transport tR(s2, io);
    auto acc = make_acceptor(io);
    uint16_t port = acc.local_endpoint().port();
    run_acceptor(io, acc, tR);

    std::promise<skiffy::message> prom;
    auto fut = prom.get_future();
    bool fired = false;
    tR.on_message([&](const skiffy::message& m) {
        if (!fired) {
            fired = true;
            prom.set_value(m);
        }
    });

    skiffy::asio_transport tS(s1, io);
    skiffy::server_id peer_id({127, 0, 0, 1}, port);
    tS.add_peer(peer_id);

    std::thread th([&] { io.run_for(5s); });

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

    io.stop();
    th.join();
}

TEST_CASE("asio_transport bidirectional exchange") {
    using namespace std::chrono_literals;
    asio::io_context io;

    skiffy::asio_transport tA(s1, io);
    skiffy::asio_transport tB(s2, io);

    auto accA = make_acceptor(io);
    auto accB = make_acceptor(io);
    uint16_t portA = accA.local_endpoint().port();
    uint16_t portB = accB.local_endpoint().port();
    run_acceptor(io, accA, tA);
    run_acceptor(io, accB, tB);

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

    tA.add_peer(skiffy::server_id({127, 0, 0, 1}, portB));
    tB.add_peer(skiffy::server_id({127, 0, 0, 1}, portA));

    std::thread th([&] { io.run_for(5s); });

    skiffy::message req;
    req.type = skiffy::msg_type::request_vote_req;
    req.term = 10;
    req.from = s1;
    req.to = skiffy::server_id({127, 0, 0, 1}, portB);
    tA.send(req);

    skiffy::message resp;
    resp.type = skiffy::msg_type::request_vote_resp;
    resp.term = 10;
    resp.from = s2;
    resp.to = skiffy::server_id({127, 0, 0, 1}, portA);
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

    io.stop();
    th.join();
}

// -------------------------------------------------------
// Group 3 — error path coverage
// -------------------------------------------------------

TEST_CASE("asio_transport: connection failure is silent") {
    using namespace std::chrono_literals;
    asio::io_context io;
    skiffy::asio_transport t(s1, io);

    // grab a free port then stop listening so connect fails
    asio::ip::tcp::acceptor acc(io);
    acc.open(asio::ip::tcp::v4());
    acc.bind({asio::ip::make_address("127.0.0.1"), 0});
    uint16_t port = acc.local_endpoint().port();
    acc.close();

    skiffy::server_id closed_id({127, 0, 0, 1}, port);
    t.add_peer(closed_id);
    skiffy::message msg;
    msg.type = skiffy::msg_type::request_vote_req;
    msg.term = 1;
    msg.from = s1;
    msg.to = closed_id;
    t.send(msg);

    // async_connect fires with ec set; evt_fail -> disc, no crash
    io.run_for(500ms);
    CHECK(true);
}

TEST_CASE("asio_transport: read error on peer disconnect") {
    using namespace std::chrono_literals;
    asio::io_context io;

    skiffy::asio_transport t(s2, io);
    auto acc = make_acceptor(io);
    uint16_t port = acc.local_endpoint().port();
    run_acceptor(io, acc, t);

    // raw client: connect, send tag 0x01, then close immediately
    {
        asio::io_context tmp;
        asio::ip::tcp::socket s(tmp);
        asio::error_code ec;
        s.connect({asio::ip::make_address("127.0.0.1"), port}, ec);
        if (!ec) {
            const uint8_t tag =
                static_cast<uint8_t>(skiffy::protocol_tag::raft);
            asio::write(s, asio::buffer(&tag, 1), ec);
        }
    } // socket destructs -> EOF on receiver's do_read_loop

    // do_read_loop callback fires with ec set, logs, returns
    io.run_for(500ms);
    CHECK(true);
}
