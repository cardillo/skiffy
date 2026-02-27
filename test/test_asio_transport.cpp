#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include "doctest/doctest.h"

#include "raftpp.h"

// -------------------------------------------------------
// helpers
// -------------------------------------------------------

static void run_acceptor(asio::io_context& io, asio::ip::tcp::acceptor& acc,
                         raftpp::asio_transport& t) {
    auto sock = std::make_shared<asio::ip::tcp::socket>(io);
    acc.async_accept(*sock, [&io, &acc, &t, sock](asio::error_code ec) {
        if (!ec) {
            auto tag = std::make_shared<std::array<uint8_t, 1>>();
            asio::async_read(
                *sock, asio::buffer(*tag),
                [&t, sock, tag](asio::error_code e2, size_t) {
                    if (e2)
                        return;
                    if (static_cast<raftpp::protocol_tag>((*tag)[0]) ==
                        raftpp::protocol_tag::raft)
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
    raftpp::asio_transport t(1, io);

    raftpp::message msg;
    msg.type = raftpp::msg_type::request_vote_req;
    msg.term = 1;
    msg.from = 1;
    msg.to = 99;

    t.send(msg);
    CHECK(true);
}

TEST_CASE("asio_transport remove peer: subsequent send is silent") {
    asio::io_context io;
    raftpp::asio_transport t(1, io);

    asio::ip::tcp::endpoint ep{asio::ip::make_address("127.0.0.1"), 19999};
    t.add_peer(2, ep);
    t.remove_peer(2);

    raftpp::message msg;
    msg.type = raftpp::msg_type::request_vote_req;
    msg.term = 1;
    msg.from = 1;
    msg.to = 2;

    t.send(msg);
    CHECK(true);
}

// -------------------------------------------------------
// Group 2 — loopback TCP integration
// -------------------------------------------------------

TEST_CASE("asio_transport delivers one message over loopback") {
    using namespace std::chrono_literals;
    asio::io_context io;

    raftpp::asio_transport tR(2, io);
    auto acc = make_acceptor(io);
    uint16_t port = acc.local_endpoint().port();
    run_acceptor(io, acc, tR);

    std::promise<raftpp::message> prom;
    auto fut = prom.get_future();
    bool fired = false;
    tR.set_callback([&](const raftpp::message& m) {
        if (!fired) {
            fired = true;
            prom.set_value(m);
        }
    });

    raftpp::asio_transport tS(1, io);
    tS.add_peer(2, {asio::ip::make_address("127.0.0.1"), port});

    std::thread th([&] { io.run_for(5s); });

    raftpp::message out;
    out.type = raftpp::msg_type::request_vote_req;
    out.term = 7;
    out.from = 1;
    out.to = 2;
    tS.send(out);

    auto st = fut.wait_for(3s);
    REQUIRE(st == std::future_status::ready);
    auto got = fut.get();
    CHECK(got.term == 7);
    CHECK(got.type == raftpp::msg_type::request_vote_req);

    io.stop();
    th.join();
}

TEST_CASE("asio_transport delivers multiple messages in order") {
    using namespace std::chrono_literals;
    asio::io_context io;

    raftpp::asio_transport tR(2, io);
    auto acc = make_acceptor(io);
    uint16_t port = acc.local_endpoint().port();
    run_acceptor(io, acc, tR);

    std::promise<std::vector<raftpp::message>> prom;
    auto fut = prom.get_future();
    std::vector<raftpp::message> received;
    bool fired = false;
    tR.set_callback([&](const raftpp::message& m) {
        received.push_back(m);
        if (!fired && received.size() == 3) {
            fired = true;
            prom.set_value(received);
        }
    });

    raftpp::asio_transport tS(1, io);
    tS.add_peer(2, {asio::ip::make_address("127.0.0.1"), port});

    std::thread th([&] { io.run_for(5s); });

    for (raftpp::term_t term = 1; term <= 3; ++term) {
        raftpp::message msg;
        msg.type = raftpp::msg_type::append_entries_req;
        msg.term = term;
        msg.from = 1;
        msg.to = 2;
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

    raftpp::asio_transport tR(2, io);
    auto acc = make_acceptor(io);
    uint16_t port = acc.local_endpoint().port();
    run_acceptor(io, acc, tR);

    std::promise<raftpp::message> prom;
    auto fut = prom.get_future();
    bool fired = false;
    tR.set_callback([&](const raftpp::message& m) {
        if (!fired) {
            fired = true;
            prom.set_value(m);
        }
    });

    raftpp::asio_transport tS(1, io);
    tS.add_peer(2, {asio::ip::make_address("127.0.0.1"), port});

    std::thread th([&] { io.run_for(5s); });

    raftpp::message out;
    out.type = raftpp::msg_type::request_vote_resp;
    out.term = 4;
    out.from = 1;
    out.to = 2;
    out.vote_granted = true;
    out.last_log_index = 5;
    out.last_log_term = 3;
    tS.send(out);

    auto st = fut.wait_for(3s);
    REQUIRE(st == std::future_status::ready);
    auto got = fut.get();
    CHECK(got.type == raftpp::msg_type::request_vote_resp);
    CHECK(got.term == 4);
    CHECK(got.from == 1);
    CHECK(got.to == 2);
    CHECK(got.vote_granted == true);
    CHECK(got.last_log_index == 5);
    CHECK(got.last_log_term == 3);

    io.stop();
    th.join();
}

TEST_CASE("asio_transport bidirectional exchange") {
    using namespace std::chrono_literals;
    asio::io_context io;

    raftpp::asio_transport tA(1, io);
    raftpp::asio_transport tB(2, io);

    auto accA = make_acceptor(io);
    auto accB = make_acceptor(io);
    uint16_t portA = accA.local_endpoint().port();
    uint16_t portB = accB.local_endpoint().port();
    run_acceptor(io, accA, tA);
    run_acceptor(io, accB, tB);

    std::promise<raftpp::message> promB;
    auto futB = promB.get_future();
    bool firedB = false;
    tB.set_callback([&](const raftpp::message& m) {
        if (!firedB) {
            firedB = true;
            promB.set_value(m);
        }
    });

    std::promise<raftpp::message> promA;
    auto futA = promA.get_future();
    bool firedA = false;
    tA.set_callback([&](const raftpp::message& m) {
        if (!firedA) {
            firedA = true;
            promA.set_value(m);
        }
    });

    tA.add_peer(2, {asio::ip::make_address("127.0.0.1"), portB});
    tB.add_peer(1, {asio::ip::make_address("127.0.0.1"), portA});

    std::thread th([&] { io.run_for(5s); });

    raftpp::message req;
    req.type = raftpp::msg_type::request_vote_req;
    req.term = 10;
    req.from = 1;
    req.to = 2;
    tA.send(req);

    raftpp::message resp;
    resp.type = raftpp::msg_type::request_vote_resp;
    resp.term = 10;
    resp.from = 2;
    resp.to = 1;
    resp.vote_granted = true;
    tB.send(resp);

    auto stB = futB.wait_for(3s);
    REQUIRE(stB == std::future_status::ready);
    auto gotB = futB.get();
    CHECK(gotB.term == 10);
    CHECK(gotB.type == raftpp::msg_type::request_vote_req);

    auto stA = futA.wait_for(3s);
    REQUIRE(stA == std::future_status::ready);
    auto gotA = futA.get();
    CHECK(gotA.term == 10);
    CHECK(gotA.type == raftpp::msg_type::request_vote_resp);
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
    raftpp::asio_transport t(1, io);

    // grab a free port then stop listening so connect fails
    asio::ip::tcp::acceptor acc(io);
    acc.open(asio::ip::tcp::v4());
    acc.bind({asio::ip::make_address("127.0.0.1"), 0});
    uint16_t port = acc.local_endpoint().port();
    acc.close();

    t.add_peer(2, {asio::ip::make_address("127.0.0.1"), port});
    raftpp::message msg;
    msg.type = raftpp::msg_type::request_vote_req;
    msg.term = 1;
    msg.from = 1;
    msg.to = 2;
    t.send(msg);

    // async_connect fires with ec set; evt_fail -> disc, no crash
    io.run_for(500ms);
    CHECK(true);
}

TEST_CASE("asio_transport: read error on peer disconnect") {
    using namespace std::chrono_literals;
    asio::io_context io;

    raftpp::asio_transport t(2, io);
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
                static_cast<uint8_t>(raftpp::protocol_tag::raft);
            asio::write(s, asio::buffer(&tag, 1), ec);
        }
    } // socket destructs -> EOF on receiver's do_read_loop

    // do_read_loop callback fires with ec set, logs, returns
    io.run_for(500ms);
    CHECK(true);
}
