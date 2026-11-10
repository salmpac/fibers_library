#pragma once

// fibers::metrics: counters, gauges and histograms with a Prometheus text
// exposition (registry.hpp), the library's own collectors (collectors.hpp)
// and the /metrics endpoint for fibers::http (http.hpp, needs fibers::http).

#include "fibers/metrics/exposition.hpp"
#include "fibers/metrics/instruments.hpp"
#include "fibers/metrics/registry.hpp"
