#pragma once
#include "raftpp.h"

namespace raftpp {

// Deliver a snapshot of sent messages through fn,
// then clear the transport.
template<typename Fn>
void deliver(
    memory_transport& t, Fn&& fn)
{
    auto msgs = t.sent;
    t.clear();
    for (auto& m : msgs)
        std::forward<Fn>(fn)(m);
}

} // namespace raftpp
