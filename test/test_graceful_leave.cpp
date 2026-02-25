#include <chrono>
#include <future>
#include <thread>

#include "doctest/doctest.h"

#include "raftpp.h"

using namespace raftpp;

// -------------------------------------------------------
// server::remove_peer
// -------------------------------------------------------

TEST_CASE("remove_peer cleans up server state") {
    memory_transport t;
    server<memory_transport> s(1, {2, 3}, t);

    REQUIRE(s.peers().count(2) == 1);
    REQUIRE(s.peers().count(3) == 1);

    // become leader so next_index / match_index
    // are initialised for all peers
    s.timeout();
    auto grant = [&](server_id src) {
        message rv;
        rv.type = msg_type::request_vote_resp;
        rv.term = s.current_term();
        rv.from = src;
        rv.to = 1;
        rv.vote_granted = true;
        s.receive(rv);
    };
    grant(2);
    grant(3);
    s.become_leader();
    REQUIRE(s.state() == server_state::leader);

    s.remove_peer(2);

    CHECK(s.peers().count(2) == 0);
    CHECK(s.peers().count(3) == 1);
    // next_index / match_index gone for peer 2
    bool threw2 = false;
    try {
        s.next_index_for(2);
    } catch (...) { threw2 = true; }
    CHECK(threw2);

    bool threw3 = false;
    try {
        s.next_index_for(3);
    } catch (...) { threw3 = true; }
    CHECK(!threw3);
}

// -------------------------------------------------------
// codec round-trips
// -------------------------------------------------------

TEST_CASE("leave_req serialises round-trip") {
    mem_message msg;
    msg.type = mem_msg_type::leave_req;
    msg.joiner_id = 2;

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, msg);
    msgpack::object_handle oh = msgpack::unpack(sbuf.data(), sbuf.size());
    mem_message msg2;
    oh.get().convert(msg2);

    CHECK(msg2.type == mem_msg_type::leave_req);
    CHECK(msg2.joiner_id.value_or(0) == 2);
    CHECK(!msg2.members.has_value());
}

TEST_CASE("remove serialises round-trip") {
    mem_message msg;
    msg.type = mem_msg_type::remove;
    msg.joiner_id = 3;

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, msg);
    msgpack::object_handle oh = msgpack::unpack(sbuf.data(), sbuf.size());
    mem_message msg2;
    oh.get().convert(msg2);

    CHECK(msg2.type == mem_msg_type::remove);
    CHECK(msg2.joiner_id.value_or(0) == 3);
    CHECK(!msg2.members.has_value());
}

// -------------------------------------------------------
// membership_manager leave/remove flow
// -------------------------------------------------------

TEST_CASE("membership_manager handles leave_req") {
    using namespace asio;
    using tcp = ip::tcp;

    // manager A listens on mem port 19301
    io_context ioA;
    asio_transport tA(1, ioA);
    tA.listen(tcp::endpoint(ip::make_address("127.0.0.1"), 19301));

    membership_manager mgrA(1, ioA, tA);
    mgrA.set_self_info("127.0.0.1", 19301, 19401);
    mgrA.listen(tcp::endpoint(ip::make_address("127.0.0.1"), 19401));

    // add peer 2 to A's member list manually
    // so there is something to remove
    {
        member_info mi;
        mi.id = 2;
        mi.host = "127.0.0.1";
        mi.raft_port = 19302;
        mi.mem_port = 19402;
        // access via join so that members_ is
        // populated; use a synchronous send
        // directly via a raw socket instead
        asio::io_context tmp;
        asio::ip::tcp::socket s(tmp);
        asio::error_code ec;
        s.connect(tcp::endpoint(ip::make_address("127.0.0.1"), 19401), ec);
        if (!ec) {
            mem_message ann;
            ann.type = mem_msg_type::announce;
            ann.joiner_id = 2;
            ann.joiner_host = "127.0.0.1";
            ann.joiner_raft_port = 19302;
            ann.joiner_mem_port = 19402;
            msgpack::sbuffer ann_sbuf;
            msgpack::pack(ann_sbuf, ann);
            asio::write(s, asio::buffer(ann_sbuf.data(), ann_sbuf.size()),
                        ec);
        }
    }

    std::promise<server_id> prom;
    auto fut = prom.get_future();
    bool set = false;
    mgrA.set_on_peer_removed([&](server_id id) {
        if (!set) {
            set = true;
            prom.set_value(id);
        }
    });

    std::thread thA([&] { ioA.run_for(std::chrono::seconds(5)); });

    // wait briefly for the announce to be
    // processed
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // B sends leave_req
    {
        asio::io_context tmp;
        asio::ip::tcp::socket s(tmp);
        asio::error_code ec;
        s.connect(tcp::endpoint(ip::make_address("127.0.0.1"), 19401), ec);
        if (!ec) {
            mem_message lv;
            lv.type = mem_msg_type::leave_req;
            lv.joiner_id = 2;
            msgpack::sbuffer lv_sbuf;
            msgpack::pack(lv_sbuf, lv);
            asio::write(s, asio::buffer(lv_sbuf.data(), lv_sbuf.size()), ec);
        }
    }

    auto status = fut.wait_for(std::chrono::seconds(5));
    REQUIRE(status == std::future_status::ready);
    CHECK(fut.get() == 2);

    // member 2 should be gone from A's list
    bool found = false;
    for (auto& m : mgrA.members())
        if (m.id == 2)
            found = true;
    CHECK(!found);

    ioA.stop();
    thA.join();
}

TEST_CASE("membership_manager handles remove") {
    using namespace asio;
    using tcp = ip::tcp;

    io_context ioA;
    asio_transport tA(1, ioA);
    tA.listen(tcp::endpoint(ip::make_address("127.0.0.1"), 19305));

    membership_manager mgrA(1, ioA, tA);
    mgrA.set_self_info("127.0.0.1", 19305, 19405);
    mgrA.listen(tcp::endpoint(ip::make_address("127.0.0.1"), 19405));

    // seed member 3 via announce
    {
        asio::io_context tmp;
        asio::ip::tcp::socket s(tmp);
        asio::error_code ec;
        s.connect(tcp::endpoint(ip::make_address("127.0.0.1"), 19405), ec);
        if (!ec) {
            mem_message ann;
            ann.type = mem_msg_type::announce;
            ann.joiner_id = 3;
            ann.joiner_host = "127.0.0.1";
            ann.joiner_raft_port = 19306;
            ann.joiner_mem_port = 19406;
            msgpack::sbuffer ann_sbuf;
            msgpack::pack(ann_sbuf, ann);
            asio::write(s, asio::buffer(ann_sbuf.data(), ann_sbuf.size()),
                        ec);
        }
    }

    std::promise<server_id> prom;
    auto fut = prom.get_future();
    bool set = false;
    mgrA.set_on_peer_removed([&](server_id id) {
        if (!set) {
            set = true;
            prom.set_value(id);
        }
    });

    std::thread thA([&] { ioA.run_for(std::chrono::seconds(5)); });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // send remove message
    {
        asio::io_context tmp;
        asio::ip::tcp::socket s(tmp);
        asio::error_code ec;
        s.connect(tcp::endpoint(ip::make_address("127.0.0.1"), 19405), ec);
        if (!ec) {
            mem_message rm;
            rm.type = mem_msg_type::remove;
            rm.joiner_id = 3;
            msgpack::sbuffer rm_sbuf;
            msgpack::pack(rm_sbuf, rm);
            asio::write(s, asio::buffer(rm_sbuf.data(), rm_sbuf.size()), ec);
        }
    }

    auto status = fut.wait_for(std::chrono::seconds(5));
    REQUIRE(status == std::future_status::ready);
    CHECK(fut.get() == 3);

    bool found = false;
    for (auto& m : mgrA.members())
        if (m.id == 3)
            found = true;
    CHECK(!found);

    ioA.stop();
    thA.join();
}
