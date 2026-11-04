# Local patches to Twist

Applied on top of upstream Twist at the pinned commit
`407b44e51a9d53c9fd5cbada08ada7a4a1cf029d`
(https://gitlab.com/Lipovsky/twist), at build time:

- nix: `flake.nix` builds `FIBERS_DEP_TWIST` with `pkgs.applyPatches`
  (dev shell and packages alike);
- without nix: `cmake/Twist.cmake` fetches the same commit and applies them
  with `git apply` (`cmake/TwistPatch.cmake`, idempotent on re-configure).

| Patch | What |
|-------|------|
| `0001-sim-support-atomic_thread_fence-seq_cst.patch` | `atomic_thread_fence(seq_cst)` in the simulator (it used to panic with "Fence not supported") |
| `0002-sim-opt-in-store-buffering-x86-TSO-memory-model.patch` | opt-in x86-TSO store buffers (`TWIST_SIM_STORE_BUFFERING` / `SimulatorParams::store_buffering`): store -> load reordering, so a missing seq_cst fence becomes observable |
| `0003-tests-sim-tso-litmus-and-protocol-tests-for-store-bu.patch` | Twist's own tests for the mode (litmus, drain, wake-up protocols) |
| `0004-docs-store-buffering-x86-TSO-mode.patch` | Twist's docs for the mode |

They come from the `sim-tso-store-buffer` branch of a local Twist checkout
(`git format-patch 407b44e5..`, without the MR description commit) and are
meant for upstream. Drop them, and bump the pin, once upstream merges an
equivalent.

Only Twist itself is patched. Its dependencies (sure, sure-stack, futex_like,
...) are pinned only by `flake.lock`; a non-nix build fetches them at whatever
revision Twist's CMake asks for.

## Authorship and licensing

Twist is written by Roman Lipovsky (https://gitlab.com/Lipovsky/twist); all
rights to Twist belong to its author. At the time of writing the upstream
repository carries no license file, so these patches (which contain fragments
of Twist's code as diff context and changes derived from it) are published
here solely to build and test this project. They are intended to be proposed
upstream as a merge request; if the author objects to them being published
here, they will be removed.
