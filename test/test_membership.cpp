#include <chrono>
#include <future>
#include <thread>

#include "doctest/doctest.h"

#include "raftpp.h"
#include "test_utils.h"

using namespace raftpp;

// -------------------------------------------------------
// membership codec tests
// -------------------------------------------------------

TEST_CASE("member_info encode/decode roundtrip") {
    member_info mi;
    mi.addr = server_id("127.0.0.1:9001");

    mem_message msg;
    msg.type = mem_msg_type::join_resp;
    msg.members = std::vector<member_info>{mi};

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, msg);
    msgpack::object_handle oh = msgpack::unpack(sbuf.data(), sbuf.size());
    mem_message msg2;
    oh.get().convert(msg2);

    REQUIRE(msg2.members.has_value());
    REQUIRE(msg2.members->size() == 1);
    CHECK(msg2.members->at(0).addr == mi.addr);
}

TEST_CASE("mem_message join_req encode/decode") {
    mem_message msg;
    msg.type = mem_msg_type::join_req;
    msg.joiner_addr = server_id("127.0.0.1:9002");

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, msg);
    msgpack::object_handle oh = msgpack::unpack(sbuf.data(), sbuf.size());
    mem_message msg2;
    oh.get().convert(msg2);

    CHECK(msg2.type == mem_msg_type::join_req);
    CHECK(msg2.joiner_addr.value_or(nil_id) ==
          server_id("127.0.0.1:9002"));
    CHECK(!msg2.members.has_value());
}

TEST_CASE("mem_message join_resp encode/decode") {
    member_info mi1;
    mi1.addr = server_id("127.0.0.1:9001");

    member_info mi2;
    mi2.addr = server_id("127.0.0.1:9002");

    mem_message msg;
    msg.type = mem_msg_type::join_resp;
    msg.members = std::vector<member_info>{mi1, mi2};

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, msg);
    msgpack::object_handle oh = msgpack::unpack(sbuf.data(), sbuf.size());
    mem_message msg2;
    oh.get().convert(msg2);

    CHECK(msg2.type == mem_msg_type::join_resp);
    REQUIRE(msg2.members.has_value());
    CHECK(msg2.members->size() == 2);
    CHECK(msg2.members->at(0).addr == server_id("127.0.0.1:9001"));
    CHECK(msg2.members->at(1).addr == server_id("127.0.0.1:9002"));
}

TEST_CASE("mem_message announce encode/decode") {
    mem_message msg;
    msg.type = mem_msg_type::announce;
    msg.joiner_addr = server_id("192.168.1.1:9003");

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, msg);
    msgpack::object_handle oh = msgpack::unpack(sbuf.data(), sbuf.size());
    mem_message msg2;
    oh.get().convert(msg2);

    CHECK(msg2.type == mem_msg_type::announce);
    CHECK(msg2.joiner_addr.value_or(nil_id) ==
          server_id("192.168.1.1:9003"));
    CHECK(!msg2.members.has_value());
}

// -------------------------------------------------------
// server::add_peer tests
// -------------------------------------------------------

TEST_CASE("add_peer grows peer set") {
    memory_transport t;
    server<memory_transport> s(s1, {}, t);

    CHECK(s.peers().empty());
    s.add_peer(s2);
    CHECK(s.peers().size() == 1);
    CHECK(s.peers().count(s2) == 1);
    s.add_peer(s3);
    CHECK(s.peers().size() == 2);
}

TEST_CASE("add_peer initialises leader vars") {
    memory_transport t;
    server<memory_transport> s(s1, {}, t);
    s.add_peer(s2);

    CHECK(s.next_index_for(s2) == 1);
    CHECK(s.match_index_for(s2) == 0);
}

TEST_CASE("add_peer after log entries: next_index correct") {
    memory_transport t;
    // 3-node cluster: need 2 votes (majority of 3)
    server<memory_transport> s(s1, {s2, s3}, t);

    s.timeout();

    auto grant = [&](server_id src) {
        message rv;
        rv.type = msg_type::request_vote_resp;
        rv.term = s.current_term();
        rv.from = src;
        rv.to = s1;
        rv.vote_granted = true;
        s.receive(rv);
    };
    grant(s2);
    grant(s3);

    s.become_leader();
    REQUIRE(s.state() == server_state::leader);

    s.client_request("x");
    // log now has 1 entry; add a 4th peer
    s.add_peer(s4);

    CHECK(s.next_index_for(s4) == 2);
    CHECK(s.match_index_for(s4) == 0);
}

TEST_CASE("quorum recalculates after add_peer") {
    memory_transport t;
    // start solo: cluster size=1, any 1 vote is quorum
    server<memory_transport> s(s1, {}, t);

    std::set<server_id> just_self{s1};
    CHECK(s.is_quorum(just_self));

    s.add_peer(s2);
    // cluster size=2: need >1 vote, so {s1} is not quorum
    CHECK(!s.is_quorum(just_self));

    std::set<server_id> both{s1, s2};
    CHECK(s.is_quorum(both));
}

TEST_CASE("leader crash before config_joint"
          " committed reverts membership") {
    memory_transport t;
    server<memory_transport> sv1(s1, {s2, s3}, t);
    server<memory_transport> sv2(s2, {s1, s3}, t);
    server<memory_transport> sv3(s3, {s1, s2}, t);

    // elect s1 as leader (term 2)
    sv1.timeout();
    message v;
    v.type = msg_type::request_vote_resp;
    v.term = sv1.current_term();
    v.vote_granted = true;
    v.to = s1;
    v.from = s2;
    sv1.receive(v);
    v.from = s3;
    sv1.receive(v);
    sv1.become_leader();
    t.clear();

    // initiate config change before replicating
    sv1.config_request(
        {s2, s3, s4});
    REQUIRE(sv1.joint_config().has_value());
    REQUIRE(sv1.peers().count(s4) == 1);

    // s1 crashes before config_joint is
    // replicated or committed
    sv1.restart();
    t.clear();

    // s2 wins election at term 3 with a
    // client entry (term=3) that will conflict
    // with s1's config_joint (term=2)
    sv2.timeout(); // term 2
    sv2.timeout(); // term 3
    message v2;
    v2.type = msg_type::request_vote_resp;
    v2.term = sv2.current_term();
    v2.vote_granted = true;
    v2.to = s2;
    v2.from = s3;
    sv2.receive(v2);
    sv2.become_leader();
    t.clear();

    sv2.client_request("x");
    sv2.append_entries(s1);
    // deliver s2's AE to s1; s1 truncates the
    // stale config_joint entry on conflict
    t.deliver([&](const message& m) {
        if (m.to == s1)
            sv1.receive(m);
    });

    CHECK(!sv1.joint_config().has_value());
    CHECK(!sv1.peers().count(s4));
    CHECK(sv1.peers().count(s2) == 1);
    CHECK(sv1.peers().count(s3) == 1);
}

// -------------------------------------------------------
// membership_manager join flow test
// -------------------------------------------------------

// Shared-port router helper (mirrors cluster_node's do_route).
static void do_accept_route(asio::io_context& io,
                            asio::ip::tcp::acceptor& acc, asio_transport& t,
                            membership_manager& mgr);

static void route_one(const std::shared_ptr<asio::ip::tcp::socket>& sock,
                      asio_transport& t, membership_manager& mgr) {
    auto tag = std::make_shared<std::array<uint8_t, 1>>();
    asio::async_read(
        *sock, asio::buffer(*tag),
        [sock, tag, &t, &mgr](asio::error_code e, size_t) {
            if (e)
                return;
            if (static_cast<raftpp::protocol_tag>((*tag)[0]) ==
                raftpp::protocol_tag::raft)
                t.accept_connection(sock);
            else if (static_cast<raftpp::protocol_tag>((*tag)[0]) ==
                     raftpp::protocol_tag::membership)
                mgr.accept_connection(sock);
        });
}

static void do_accept_route(asio::io_context& io,
                            asio::ip::tcp::acceptor& acc, asio_transport& t,
                            membership_manager& mgr) {
    auto sock = std::make_shared<asio::ip::tcp::socket>(io);
    acc.async_accept(*sock, [&io, &acc, &t, &mgr, sock](asio::error_code ec) {
        if (!ec)
            route_one(sock, t, mgr);
        do_accept_route(io, acc, t, mgr);
    });
}

TEST_CASE("membership_manager: join flow") {
    using namespace asio;
    using tcp = ip::tcp;

    // bootstrap node (id=1) — shared single port 19101
    io_context io1;
    asio_transport t1(server_id("127.0.0.1:19101"), io1);
    membership_manager mgr1(server_id("127.0.0.1:19101"), io1, t1);
    mgr1.self_info("127.0.0.1", 19101);

    tcp::acceptor acc1(io1);
    acc1.open(tcp::v4());
    acc1.set_option(tcp::acceptor::reuse_address(true));
    acc1.bind(tcp::endpoint(ip::make_address("127.0.0.1"), 19101));
    acc1.listen();
    do_accept_route(io1, acc1, t1, mgr1);

    // promise is set by the first on_peer_added
    // callback fired on the io1 thread; future is
    // read on the main thread — no data race
    std::promise<server_id> peer_promise;
    auto peer_future = peer_promise.get_future();
    bool promise_set = false;
    mgr1.on_peer_added([&](server_id id, const tcp::endpoint&) {
        if (!promise_set) {
            promise_set = true;
            peer_promise.set_value(id);
        }
    });

    // run bootstrap io_context in background
    std::thread th1([&] { io1.run(); });

    // joining node (id=2)
    io_context io2;
    asio_transport t2(server_id("127.0.0.1:19102"), io2);
    membership_manager mgr2(server_id("127.0.0.1:19102"), io2, t2);
    mgr2.self_info("127.0.0.1", 19102);

    // join via the bootstrap raft port
    mgr2.join(tcp::endpoint(ip::make_address("127.0.0.1"), 19101));

    // wait for bootstrap to process the announce
    auto status = peer_future.wait_for(std::chrono::seconds(5));
    REQUIRE(status == std::future_status::ready);
    server_id peer2_added_on_1 = peer_future.get();

    // joiner should have 2 members (bootstrap + self)
    CHECK(mgr2.members().size() == 2);

    // joiner should see bootstrap (id=1) in member list
    std::string mgr1_addr = "127.0.0.1:19101";
    bool found1 = false;
    for (auto& m : mgr2.members()) {
        if (m.addr == server_id(mgr1_addr)) {
            found1 = true;
            break;
        }
    }
    CHECK(found1);

    // bootstrap should have added the joiner
    CHECK(peer2_added_on_1 ==
          server_id("127.0.0.1:19102"));

    // bootstrap member list should also have 2 entries
    CHECK(mgr1.members().size() == 2);

    io1.stop();
    th1.join();
}

// -------------------------------------------------------
// membership_manager integration — branch coverage
// -------------------------------------------------------

static asio::ip::tcp::acceptor make_mem_acceptor(asio::io_context& io) {
    using tcp = asio::ip::tcp;
    tcp::acceptor acc(io);
    acc.open(tcp::v4());
    acc.set_option(tcp::acceptor::reuse_address(true));
    acc.bind({asio::ip::make_address("127.0.0.1"), 0});
    acc.listen();
    return acc;
}

static void run_mgr_acceptor(asio::io_context& io,
                             asio::ip::tcp::acceptor& acc,
                             membership_manager& mgr) {
    auto sock = std::make_shared<asio::ip::tcp::socket>(io);
    acc.async_accept(*sock, [&io, &acc, &mgr, sock](asio::error_code ec) {
        if (!ec) {
            auto tag = std::make_shared<std::array<uint8_t, 1>>();
            asio::async_read(
                *sock, asio::buffer(*tag),
                [&mgr, sock, tag](asio::error_code e2, size_t) {
                    if (e2)
                        return;
                    if (static_cast<raftpp::protocol_tag>((*tag)[0]) ==
                        raftpp::protocol_tag::membership)
                        mgr.accept_connection(sock);
                });
        }
        run_mgr_acceptor(io, acc, mgr);
    });
}

static void send_raw_mem_msg(uint16_t port, const mem_message& msg) {
    asio::io_context tmp;
    asio::ip::tcp::socket s(tmp);
    asio::error_code ec;
    s.connect({asio::ip::make_address("127.0.0.1"), port}, ec);
    if (ec)
        return;
    const uint8_t tag =
        static_cast<uint8_t>(raftpp::protocol_tag::membership);
    asio::write(s, asio::buffer(&tag, 1), ec);
    if (ec)
        return;
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, msg);
    asio::write(s, asio::buffer(sbuf.data(), sbuf.size()), ec);
}

TEST_CASE("membership_manager: announce without on_peer_added") {
    using namespace std::chrono_literals;
    asio::io_context io;
    asio_transport t(server_id("127.0.0.1:19101"), io);
    membership_manager mgr(server_id("127.0.0.1:19101"), io, t);
    mgr.self_info("127.0.0.1", 0);

    auto acc = make_mem_acceptor(io);
    uint16_t port = acc.local_endpoint().port();
    run_mgr_acceptor(io, acc, mgr);
    // no on_peer_added set — covers if (on_peer_added_) false
    std::thread th([&] { io.run_for(3s); });

    mem_message ann;
    ann.type = mem_msg_type::announce;
    ann.joiner_addr = server_id("127.0.0.1:9099");
    send_raw_mem_msg(port, ann);

    std::this_thread::sleep_for(200ms);
    io.stop();
    th.join();

    bool found = false;
    for (auto& m : mgr.members())
        if (m.addr == server_id("127.0.0.1:9099"))
            found = true;
    CHECK(found);
}

TEST_CASE("membership_manager: remove without on_peer_removed") {
    using namespace std::chrono_literals;
    asio::io_context io;
    asio_transport t(server_id("127.0.0.1:19101"), io);
    membership_manager mgr(server_id("127.0.0.1:19101"), io, t);
    mgr.self_info("127.0.0.1", 0);

    auto acc = make_mem_acceptor(io);
    uint16_t port = acc.local_endpoint().port();
    run_mgr_acceptor(io, acc, mgr);
    // no on_peer_removed set — covers if (on_peer_removed_) false
    std::thread th([&] { io.run_for(3s); });

    mem_message ann;
    ann.type = mem_msg_type::announce;
    ann.joiner_addr = server_id("127.0.0.1:9099");
    send_raw_mem_msg(port, ann);
    std::this_thread::sleep_for(100ms);

    mem_message rm;
    rm.type = mem_msg_type::remove;
    rm.joiner_addr = server_id("127.0.0.1:9099");
    send_raw_mem_msg(port, rm);
    std::this_thread::sleep_for(100ms);

    io.stop();
    th.join();

    bool found = false;
    for (auto& m : mgr.members())
        if (m.addr == server_id("127.0.0.1:9099"))
            found = true;
    CHECK(!found);
}

TEST_CASE("membership_manager: duplicate announce ignored") {
    using namespace std::chrono_literals;
    asio::io_context io;
    asio_transport t(server_id("127.0.0.1:19101"), io);
    membership_manager mgr(server_id("127.0.0.1:19101"), io, t);
    mgr.self_info("127.0.0.1", 0); // seeds self in members_

    auto acc = make_mem_acceptor(io);
    uint16_t port = acc.local_endpoint().port();
    run_mgr_acceptor(io, acc, mgr);
    std::thread th([&] { io.run_for(3s); });

    mem_message ann;
    ann.type = mem_msg_type::announce;
    ann.joiner_addr = server_id("127.0.0.1:9099");
    send_raw_mem_msg(port, ann); // adds peer 3
    std::this_thread::sleep_for(100ms);
    send_raw_mem_msg(port, ann); // duplicate: x.id==3 -> return
    std::this_thread::sleep_for(100ms);

    io.stop();
    th.join();

    int n = 0;
    for (auto& m : mgr.members())
        if (m.addr == server_id("127.0.0.1:9099"))
            ++n;
    CHECK(n == 1);
}

TEST_CASE("membership_manager: do_mem_read error on close") {
    using namespace std::chrono_literals;
    asio::io_context io;
    asio_transport t(server_id("127.0.0.1:19101"), io);
    membership_manager mgr(server_id("127.0.0.1:19101"), io, t);
    mgr.self_info("127.0.0.1", 0);

    auto acc = make_mem_acceptor(io);
    uint16_t port = acc.local_endpoint().port();
    run_mgr_acceptor(io, acc, mgr);
    std::thread th([&] { io.run_for(3s); });

    {
        asio::io_context tmp;
        asio::ip::tcp::socket s(tmp);
        asio::error_code ec;
        s.connect({asio::ip::make_address("127.0.0.1"), port}, ec);
        if (!ec) {
            const uint8_t tag =
                static_cast<uint8_t>(raftpp::protocol_tag::membership);
            asio::write(s, asio::buffer(&tag, 1), ec);
        }
    } // EOF -> if (ec) true in do_mem_read

    std::this_thread::sleep_for(200ms);
    io.stop();
    th.join();
    CHECK(true);
}

TEST_CASE("membership_manager: join fires on_peer_added"
          " on joiner") {
    using tcp = asio::ip::tcp;

    asio::io_context io1;
    asio_transport t1(server_id("127.0.0.1:19101"), io1);
    membership_manager mgr1(server_id("127.0.0.1:19101"), io1, t1);

    auto acc1 = make_mem_acceptor(io1);
    uint16_t portA = acc1.local_endpoint().port();
    mgr1.self_info("127.0.0.1", portA);
    do_accept_route(io1, acc1, t1, mgr1);

    std::thread th1([&] { io1.run(); });

    asio::io_context io2;
    asio_transport t2(server_id("127.0.0.1:19102"), io2);
    membership_manager mgr2(server_id("127.0.0.1:19102"), io2, t2);
    // portA reused for self — nobody connects back to mgr2
    mgr2.self_info("127.0.0.1", portA);

    server_id added_id;
    mgr2.on_peer_added(
        [&](server_id id, const tcp::endpoint&) { added_id = id; });

    // join() is synchronous; on_peer_added fires inside it
    mgr2.join(tcp::endpoint(asio::ip::make_address("127.0.0.1"), portA));

    // mgr1's ID is "127.0.0.1:portA" (set via self_info)
    std::string expected_id = "127.0.0.1:" + std::to_string(portA);
    CHECK(added_id == server_id(expected_id));

    io1.stop();
    th1.join();
}

TEST_CASE("new leader after crash re-appends"
          " config_final") {
    memory_transport t;
    // old config: {1,2,3}; new config: {2,3}
    server<memory_transport> sv2(s2, {s1, s3}, t);

    // step 1: s2 receives a committed AE from s1
    // (term 2) with config_joint{2,3} at index 1
    msgpack::sbuffer buf;
    std::set<server_id> nc{s2, s3};
    msgpack::pack(buf, nc);
    log_entry cj;
    cj.term = 2;
    cj.type = entry_type::config_joint;
    cj.value = std::string(buf.data(), buf.data() + buf.size());

    message ae;
    ae.type = msg_type::append_entries_req;
    ae.term = 2;
    ae.from = s1;
    ae.to = s2;
    ae.prev_log_index = 0;
    ae.prev_log_term = 0;
    ae.entries = std::vector<log_entry>{cj};
    ae.commit_index = 1;
    sv2.receive(ae);

    REQUIRE(sv2.joint_config().has_value());
    REQUIRE(sv2.log().size() == 1);
    REQUIRE(sv2.log()[0].type == entry_type::config_joint);
    t.clear();

    // step 2: s2 wins election at term 3 with
    // one vote from s3; joint quorum is satisfied
    sv2.timeout(); // term 3
    t.clear();

    message rv;
    rv.type = msg_type::request_vote_resp;
    rv.term = sv2.current_term();
    rv.from = s3;
    rv.to = s2;
    rv.vote_granted = true;
    sv2.receive(rv);

    // step 3: become_leader fires the fix —
    // config_final(term=3) appended at index 2
    sv2.become_leader();
    REQUIRE(sv2.state() == server_state::leader);
    REQUIRE(sv2.log().size() == 2);
    CHECK(sv2.log()[1].type == entry_type::config_final);
    CHECK(sv2.log()[1].term == 3);
    t.clear();

    // step 4: fake AE resp from s3 (match_index=2)
    message resp;
    resp.type = msg_type::append_entries_resp;
    resp.term = sv2.current_term();
    resp.from = s3;
    resp.to = s2;
    resp.success = true;
    resp.match_index = 2;
    sv2.receive(resp);

    // step 5: advance_commit_index commits index 2
    sv2.advance_commit_index();

    // joint consensus resolved: s1 removed
    CHECK(!sv2.joint_config().has_value());
    CHECK(sv2.peers().count(s3) == 1);
    CHECK(!sv2.peers().count(s1));
}

TEST_CASE("on_peer_added fires on config"
          " change adding a peer") {
    memory_transport t;
    // follower s2; old config {1,2,3}
    server<memory_transport> sv2(s2, {s1, s3}, t);

    server_id added_pid;
    sv2.on_peer_added([&](server_id pid) { added_pid = pid; });

    // build shared encoded new config {2,3,4}
    msgpack::sbuffer buf;
    std::set<server_id> nc{s2, s3, s4};
    msgpack::pack(buf, nc);
    std::string enc(buf.data(), buf.data() + buf.size());

    // AE1: config_joint at index 1, uncommitted
    log_entry cj;
    cj.term = 2;
    cj.type = entry_type::config_joint;
    cj.value = enc;

    message ae1;
    ae1.type = msg_type::append_entries_req;
    ae1.term = 2;
    ae1.from = s1;
    ae1.to = s2;
    ae1.prev_log_index = 0;
    ae1.prev_log_term = 0;
    ae1.entries = std::vector<log_entry>{cj};
    ae1.commit_index = 0;
    sv2.receive(ae1);

    // AE2: config_final at index 2,
    // both entries committed
    log_entry cf;
    cf.term = 2;
    cf.type = entry_type::config_final;
    cf.value = enc;

    message ae2;
    ae2.type = msg_type::append_entries_req;
    ae2.term = 2;
    ae2.from = s1;
    ae2.to = s2;
    ae2.prev_log_index = 1;
    ae2.prev_log_term = 2;
    ae2.entries = std::vector<log_entry>{cf};
    ae2.commit_index = 2;
    sv2.receive(ae2);

    // peer 4 was unknown to s2; callback fires
    CHECK(added_pid == s4);
    // peer 3 was already in peers_ — no callback
    // peer 2 is self — no callback
    CHECK(!sv2.joint_config().has_value());
    CHECK(sv2.peers().count(s4) == 1);
}

TEST_CASE("on_peer_removed fires on config"
          " change removing a peer") {
    memory_transport t;
    // 4-server cluster; config_request({2,3})
    // removes peer 4
    server<memory_transport> sv1(s1, {s2, s3, s4}, t);

    server_id removed_pid;
    sv1.on_peer_removed([&](server_id pid) { removed_pid = pid; });

    // elect s1 as leader (term 2)
    sv1.timeout();
    {
        message v;
        v.type = msg_type::request_vote_resp;
        v.term = sv1.current_term();
        v.vote_granted = true;
        v.to = s1;
        v.from = s2;
        sv1.receive(v);
        v.from = s3;
        sv1.receive(v);
    }
    sv1.become_leader();
    t.clear();

    // initiate config change: remove peer 4
    sv1.config_request({s2, s3});
    REQUIRE(sv1.joint_config().has_value());
    // config_joint is at log index 1

    // fake acks from s2 and s3 for index 1
    {
        message resp;
        resp.type = msg_type::append_entries_resp;
        resp.term = sv1.current_term();
        resp.success = true;
        resp.match_index = 1;
        resp.to = s1;
        resp.from = s2;
        sv1.receive(resp);
        resp.from = s3;
        sv1.receive(resp);
    }
    // commit config_joint; leader appends
    // config_final at index 2
    sv1.advance_commit_index();

    // fake acks from s2 and s3 for index 2
    {
        message resp;
        resp.type = msg_type::append_entries_resp;
        resp.term = sv1.current_term();
        resp.success = true;
        resp.match_index = 2;
        resp.to = s1;
        resp.from = s2;
        sv1.receive(resp);
        resp.from = s3;
        sv1.receive(resp);
    }
    // commit config_final → fires on_peer_removed_(4)
    sv1.advance_commit_index();

    CHECK(removed_pid == s4);
    CHECK(!sv1.joint_config().has_value());
    CHECK(!sv1.peers().count(s4));
}
