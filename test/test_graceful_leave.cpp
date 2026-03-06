#include <chrono>
#include <future>
#include <thread>

#include "doctest/doctest.h"

#include "skiffy.h"
#include "test_utils.h"

using skiffy::server_id;

// -------------------------------------------------------
// server::remove_peer
// -------------------------------------------------------

TEST_CASE("remove_peer cleans up server state") {
    skiffy::memory_transport t;
    skiffy::server<skiffy::memory_transport> s(s1, {s2, s3}, t);

    REQUIRE(s.peers().count(s2) == 1);
    REQUIRE(s.peers().count(s3) == 1);

    // become leader so next_index / match_index
    // are initialised for all peers
    s.timeout();
    auto grant = [&](skiffy::server_id src) {
        skiffy::message rv;
        rv.type = skiffy::msg_type::request_vote_resp;
        rv.term = s.current_term();
        rv.from = src;
        rv.to = s1;
        rv.vote_granted = true;
        s.receive(rv);
    };
    grant(s2);
    grant(s3);
    s.become_leader();
    REQUIRE(s.state() == skiffy::server_state::leader);

    s.remove_peer(s2);

    CHECK(s.peers().count(s2) == 0);
    CHECK(s.peers().count(s3) == 1);
    // next_index / match_index gone for peer s2
    bool threw2 = false;
    try {
        s.next_index_for(s2);
    } catch (...) { threw2 = true; }
    CHECK(threw2);

    bool threw3 = false;
    try {
        s.next_index_for(s3);
    } catch (...) { threw3 = true; }
    CHECK(!threw3);
}

// -------------------------------------------------------
// codec round-trips
// -------------------------------------------------------

TEST_CASE("remove serialises round-trip") {
    skiffy::mem_message msg;
    msg.type = skiffy::mem_msg_type::remove;
    msg.joiner_addr = s3;

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, msg);
    msgpack::object_handle oh = msgpack::unpack(sbuf.data(), sbuf.size());
    skiffy::mem_message msg2;
    oh.get().convert(msg2);

    CHECK(msg2.type == skiffy::mem_msg_type::remove);
    CHECK(msg2.joiner_addr.value_or(skiffy::nil_id) == s3);
    CHECK(!msg2.members.has_value());
}

// -------------------------------------------------------
// membership_manager leave/remove flow
// -------------------------------------------------------

// Helper: run a shared acceptor that routes by tag byte.
static void run_router(asio::io_context& io, asio::ip::tcp::acceptor& acc,
                       skiffy::asio_transport& t,
                       skiffy::membership_manager& mgr) {
    auto sock = std::make_shared<asio::ip::tcp::socket>(io);
    acc.async_accept(*sock, [&io, &acc, &t, &mgr, sock](asio::error_code ec) {
        if (!ec) {
            auto tag = std::make_shared<std::array<uint8_t, 1>>();
            asio::async_read(
                *sock, asio::buffer(*tag),
                [&t, &mgr, sock, tag](asio::error_code e2, size_t) {
                    if (e2)
                        return;
                    if (static_cast<skiffy::protocol_tag>((*tag)[0]) ==
                        skiffy::protocol_tag::raft)
                        t.accept_connection(sock);
                    else if (static_cast<skiffy::protocol_tag>((*tag)[0]) ==
                             skiffy::protocol_tag::membership)
                        mgr.accept_connection(sock);
                });
        }
        run_router(io, acc, t, mgr);
    });
}

TEST_CASE("membership_manager handles remove") {
    using namespace asio;
    using tcp = ip::tcp;

    io_context ioA;
    skiffy::asio_transport tA(skiffy::server_id({127, 0, 0, 1}, 19305), ioA);
    skiffy::membership_manager mgrA(skiffy::server_id({127, 0, 0, 1}, 19305),
                                    tA);
    mgrA.self_info(skiffy::server_id({127, 0, 0, 1}, 19305));

    tcp::acceptor acc(ioA);
    acc.open(tcp::v4());
    acc.set_option(tcp::acceptor::reuse_address(true));
    acc.bind(tcp::endpoint(ip::make_address("127.0.0.1"), 19305));
    acc.listen();
    run_router(ioA, acc, tA, mgrA);

    // seed member 3 via announce (tag 0x02)
    {
        asio::io_context tmp;
        asio::ip::tcp::socket s(tmp);
        asio::error_code ec;
        s.connect(tcp::endpoint(ip::make_address("127.0.0.1"), 19305), ec);
        if (!ec) {
            const uint8_t tag =
                static_cast<uint8_t>(skiffy::protocol_tag::membership);
            asio::write(s, asio::buffer(&tag, 1), ec);
            skiffy::mem_message ann;
            ann.type = skiffy::mem_msg_type::announce;
            ann.joiner_addr = server_id({127, 0, 0, 1}, 19306);
            msgpack::sbuffer ann_sbuf;
            msgpack::pack(ann_sbuf, ann);
            asio::write(s, asio::buffer(ann_sbuf.data(), ann_sbuf.size()),
                        ec);
        }
    }

    std::promise<skiffy::server_id> prom;
    auto fut = prom.get_future();
    bool set = false;
    mgrA.on_peer_removed([&](skiffy::server_id id) {
        if (!set) {
            set = true;
            prom.set_value(id);
        }
    });

    std::thread thA([&] { ioA.run_for(std::chrono::seconds(5)); });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // send remove message (tag 0x02)
    {
        asio::io_context tmp;
        asio::ip::tcp::socket s(tmp);
        asio::error_code ec;
        s.connect(tcp::endpoint(ip::make_address("127.0.0.1"), 19305), ec);
        if (!ec) {
            const uint8_t tag =
                static_cast<uint8_t>(skiffy::protocol_tag::membership);
            asio::write(s, asio::buffer(&tag, 1), ec);
            skiffy::mem_message rm;
            rm.type = skiffy::mem_msg_type::remove;
            rm.joiner_addr = server_id({127, 0, 0, 1}, 19306);
            msgpack::sbuffer rm_sbuf;
            msgpack::pack(rm_sbuf, rm);
            asio::write(s, asio::buffer(rm_sbuf.data(), rm_sbuf.size()), ec);
        }
    }

    auto status = fut.wait_for(std::chrono::seconds(5));
    REQUIRE(status == std::future_status::ready);
    CHECK(fut.get() == skiffy::server_id({127, 0, 0, 1}, 19306));

    bool found = false;
    for (auto& m : mgrA.members())
        if (m == skiffy::server_id({127, 0, 0, 1}, 19306))
            found = true;
    CHECK(!found);

    ioA.stop();
    thA.join();
}
