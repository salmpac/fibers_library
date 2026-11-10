#pragma once

// tls::Connection: net::BasicConnection with TLS capability (plaintext until
// start_tls_client / start_tls_server, or TLS from the first byte with
// connect_tls). See fibers/net/connection.hpp; it lives here because it
// needs OpenSSL (fibers::tls), which net does not.

#include "fibers/net/connection.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/tls/stream.hpp"

namespace fibers::tls {

template <net::NetScheduler Sched = sched::Scheduler>
using BasicConnection = net::BasicConnection<Sched, BasicTlsStream<Sched>>;

using Connection = BasicConnection<sched::Scheduler>;

}  // namespace fibers::tls
