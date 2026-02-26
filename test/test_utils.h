#pragma once
#include <deque>
#include <random>

#include "raftpp.h"

namespace raftpp {

// -------------------------------------------------------
// memory_transport
// -------------------------------------------------------

struct memory_transport {
    std::vector<message> sent;

    void send(const message& m) { sent.push_back(m); }
    void clear() { sent.clear(); }

    template<typename Fn>
    void deliver(Fn&& fn) {
        auto msgs = sent;
        clear();
        for (auto& m : msgs)
            std::forward<Fn>(fn)(m);
    }
};

// -------------------------------------------------------
// sim_transport
// -------------------------------------------------------

struct sim_transport {
    double drop_rate = 0.0;
    double dup_rate = 0.0;

    void send(const message& m) { queue_.push_back(m); }

    void clear() { queue_.clear(); }

    const std::deque<message>& pending() const { return queue_; }
    size_t pending_count() const { return queue_.size(); }

    template <typename Fn>
    void deliver(Fn&& fn) {
        std::deque<message> tmp;
        tmp.swap(queue_);
        std::uniform_real_distribution<double> dist{0.0, 1.0};
        for (auto& msg : tmp) {
            if (dist(rng_) < drop_rate)
                continue;
            fn(msg);
            if (dist(rng_) < dup_rate)
                fn(msg);
        }
    }

  private:
    std::deque<message> queue_;
    std::mt19937 rng_{std::random_device{}()};
};

} // namespace raftpp
