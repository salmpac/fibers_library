#pragma once

// Passing a file descriptor (eventfd, memfd of the ring, ...) to another
// process over a unix domain socket (SCM_RIGHTS). Alternative: create it before
// fork(); the child inherits it.

#include <sys/socket.h>
#include <sys/types.h>

#include <cerrno>
#include <cstring>

namespace fibers::ipc {

// Sends `fd` (plus one dummy byte). 0 or an errno value. EINTR is retried.
inline int send_fd(int socket, int fd) noexcept {
    char byte = 0;
    iovec iov{&byte, 1};
    alignas(cmsghdr) char control[CMSG_SPACE(sizeof(int))] = {};
    msghdr message{};
    message.msg_iov = &iov;
    message.msg_iovlen = 1;
    message.msg_control = control;
    message.msg_controllen = sizeof(control);
    cmsghdr* header = CMSG_FIRSTHDR(&message);
    header->cmsg_level = SOL_SOCKET;
    header->cmsg_type = SCM_RIGHTS;
    header->cmsg_len = CMSG_LEN(sizeof(int));
    std::memcpy(CMSG_DATA(header), &fd, sizeof(int));
    for (;;) {
        if (::sendmsg(socket, &message, MSG_NOSIGNAL) == 1) {
            return 0;
        }
        if (errno != EINTR) {
            return errno;
        }
    }
}

// Receives a descriptor sent by send_fd (close-on-exec). The fd, or -errno
// (-ECONNRESET if the peer closed without sending one). EINTR is retried.
inline int recv_fd(int socket) noexcept {
    char byte = 0;
    iovec iov{&byte, 1};
    alignas(cmsghdr) char control[CMSG_SPACE(sizeof(int))] = {};
    msghdr message{};
    message.msg_iov = &iov;
    message.msg_iovlen = 1;
    message.msg_control = control;
    message.msg_controllen = sizeof(control);
    for (;;) {
        const ssize_t got = ::recvmsg(socket, &message, MSG_CMSG_CLOEXEC);
        if (got < 0 && errno == EINTR) {
            continue;
        }
        if (got < 0) {
            return -errno;
        }
        break;
    }
    for (cmsghdr* header = CMSG_FIRSTHDR(&message); header != nullptr;
         header = CMSG_NXTHDR(&message, header)) {
        if (header->cmsg_level == SOL_SOCKET && header->cmsg_type == SCM_RIGHTS &&
            header->cmsg_len == CMSG_LEN(sizeof(int))) {
            int fd = -1;
            std::memcpy(&fd, CMSG_DATA(header), sizeof(int));
            return fd;
        }
    }
    return -ECONNRESET;
}

}  // namespace fibers::ipc
