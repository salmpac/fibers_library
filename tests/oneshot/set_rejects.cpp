// Must NOT compile (ctest oneshot.set_rejects_throwing_construction): a
// Promise<T>::set(args...) whose T(args...) may throw and whose arguments
// do not convert to T implicitly hits the static_assert with directions
// (set() never throws).

#include <string>
#include <string_view>

#include "fibers/fiber/oneshot.hpp"

int main() {
    fibers::fiber::Oneshot<std::string> o;
    o.promise().set(std::string_view("explicit only, and it may throw"));
    return 0;
}
