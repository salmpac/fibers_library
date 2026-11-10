{
  description = "fibers_library: work-stealing thread pool for fibers";

  # Exact revisions of everything below are pinned in flake.lock.
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    # Twist (concurrency testing) and the sources its CMake fetches.
    # Handed to CMake via FIBERS_DEP_* (see cmake/Twist.cmake): no network needed.
    #
    # Twist fetches its own deps at `master`, but its master (2026-05-22) lags
    # behind them: sure renamed its target (sure -> sure-context) and sure-stack
    # moved headers afterwards. So Lipovsky's libs are pinned to revisions
    # contemporary with the Twist commit. Bump them together.
    #
    # Twist itself gets our local patches on top (twistPatched below), so the
    # store path in FIBERS_DEP_TWIST is the patched source.
    twist = { url = "gitlab:Lipovsky/twist/407b44e51a9d53c9fd5cbada08ada7a4a1cf029d"; flake = false; };
    twist-futex-like = { url = "gitlab:Lipovsky/futex_like/d617dcb44f820476d73d1ea3ca4f987d8971a0e3"; flake = false; };
    twist-spin-wait = { url = "gitlab:Lipovsky/spin_wait"; flake = false; };
    twist-sure = { url = "gitlab:Lipovsky/sure/c60cbd9ce2c1dc4bb2e848f35d9e14e42d8c4956"; flake = false; };
    twist-sure-stack = { url = "gitlab:Lipovsky/sure-stack/0c1fd3bbe12ad51ff8b55fffd0727aecb8e01271"; flake = false; };
    twist-function2 = { url = "github:Naios/function2"; flake = false; };
    twist-fmt = { url = "github:fmtlib/fmt"; flake = false; };
    twist-json = { url = "https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz"; flake = false; };
    twist-backward = { url = "github:bombela/backward-cpp"; flake = false; };

    # tlsfuzzer (GPL-2.0, not in nixpkgs): an external tool the TLS fuzz
    # script (tests/tls/external/) runs against tls_echo. Only invoked, never
    # copied into the repo. Its matching tlslite-ng (LGPL-2.1) is pinned too.
    tlsfuzzer = { url = "github:tlsfuzzer/tlsfuzzer"; flake = false; };
    tlslite-ng = { url = "github:tlsfuzzer/tlslite-ng"; flake = false; };
  };

  outputs = { self, nixpkgs, ... }@inputs:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});

      # Pinned toolchains. Bump deliberately, never via the default `gcc`/`clang`.
      # clang major should match the clangd used by the IDE (llvm21).
      toolchains = pkgs: {
        gcc = pkgs.gcc16Stdenv;
        clang = pkgs.llvmPackages_21.stdenv;
      };

      # Upstream Twist plus our local patches (third_party/twist/patches, see the
      # README there): seq_cst fences and x86-TSO store buffering in the
      # simulator. Drop once upstream has an equivalent.
      twistPatched = pkgs: pkgs.applyPatches {
        name = "twist-patched";
        src = inputs.twist;
        patches =
          let dir = ./third_party/twist/patches;
          in map (name: dir + "/${name}")
            (builtins.filter (pkgs.lib.hasSuffix ".patch") (builtins.attrNames (builtins.readDir dir)));
      };

      # FIBERS_DEP_<NAME> = /nix/store/... for cmake/Twist.cmake
      twistDepsEnv = pkgs: {
        FIBERS_DEP_TWIST = "${twistPatched pkgs}";
        FIBERS_DEP_FUTEX_LIKE = "${inputs.twist-futex-like}";
        FIBERS_DEP_SPIN_WAIT = "${inputs.twist-spin-wait}";
        FIBERS_DEP_SURE = "${inputs.twist-sure}";
        FIBERS_DEP_SURE_STACK = "${inputs.twist-sure-stack}";
        FIBERS_DEP_FUNCTION2 = "${inputs.twist-function2}";
        FIBERS_DEP_FMT = "${inputs.twist-fmt}";
        FIBERS_DEP_JSON = "${inputs.twist-json}";
        FIBERS_DEP_BACKWARD = "${inputs.twist-backward}";
      };

      # twist: null | "sim" (deterministic simulation) | "faulty" (threads + fault injection)
      # staticMusl: a fully static build against musl (pkgsStatic) with its
      #   default gcc instead of the pinned toolchains (nixpkgs' musl clang
      #   wrapper cannot link -lgcc_eh). Checks the libc-portability paths
      #   (e.g. musl has no epoll_pwait2 wrapper: the poller uses the raw
      #   syscall there).
      mkFibers = pkgs: { compiler ? "gcc", sanitize ? "", twist ? null, staticMusl ? false }:
        let
          lib = pkgs.lib;
          host = if staticMusl then pkgs.pkgsStatic else pkgs;
          stdenv = if staticMusl then host.stdenv else (toolchains pkgs).${compiler};
          suffix = lib.optionalString (compiler != "gcc") "-${compiler}"
            + lib.optionalString (twist != null) "-twist-${twist}"
            + lib.optionalString (sanitize != "") "-${builtins.replaceStrings [ "," ] [ "-" ] sanitize}";
          twistFlags = {
            sim = [ "-DTWIST_FAULTY=ON" "-DTWIST_SIM=ON" "-DTWIST_SIM_ISOLATION=ON" "-DTWIST_SIM_STORE_BUFFERING=ON" ];
            faulty = [ "-DTWIST_FAULTY=ON" ];
          };
        in
        stdenv.mkDerivation ({
          pname = "fibers${suffix}";
          version = "0.1.0";

          src = lib.fileset.toSource {
            root = ./.;
            fileset = lib.fileset.unions [
              ./CMakeLists.txt
              ./cmake
              ./include
              ./tests
            ];
          };

          nativeBuildInputs = [ pkgs.cmake pkgs.ninja ];
          # gtest is linked into the tests at build time: a build input for
          # the static (cross) stdenv, where check inputs are not.
          buildInputs = [ host.llhttp ] ++ lib.optional staticMusl host.gtest;
          checkInputs = lib.optional (!staticMusl) pkgs.gtest;

          cmakeFlags = [
            "-DFIBERS_BUILD_TESTS=ON"
            "-DFIBERS_HTTP=ON"
          ] ++ lib.optional (sanitize != "") "-DFIBERS_SANITIZE=${sanitize}"
            ++ lib.optionals (twist != null) ([
              "-DFIBERS_TWIST=ON"
              "-DFETCHCONTENT_FULLY_DISCONNECTED=ON"
            ] ++ twistFlags.${twist});

          # Sanitizer runtimes do not get along with _FORTIFY_SOURCE.
          hardeningDisable = lib.optional (sanitize != "") "fortify";

          doCheck = true;
        } // lib.optionalAttrs (twist != null) (twistDepsEnv pkgs));

      variants = pkgs: {
        default = mkFibers pkgs { };
        tsan = mkFibers pkgs { sanitize = "thread"; };
        asan = mkFibers pkgs { sanitize = "address,undefined"; };
        clang = mkFibers pkgs { compiler = "clang"; };
        clang-tsan = mkFibers pkgs { compiler = "clang"; sanitize = "thread"; };
        clang-asan = mkFibers pkgs { compiler = "clang"; sanitize = "address,undefined"; };
        twist-sim = mkFibers pkgs { compiler = "clang"; twist = "sim"; };
        twist-faulty-tsan = mkFibers pkgs { compiler = "clang"; twist = "faulty"; sanitize = "thread"; };
        static-musl = mkFibers pkgs { staticMusl = true; };
      };
    in
    {
      packages = forAllSystems variants;

      checks = forAllSystems (pkgs: self.packages.${pkgs.stdenv.hostPlatform.system});

      devShells = forAllSystems (pkgs:
        let
          tc = toolchains pkgs;
          llvm = pkgs.llvmPackages_21;
        in
        {
          # Both pinned compilers are on PATH: g++ (gcc16) and clang++ (llvm21).
          # CMake presets pick one explicitly via CMAKE_CXX_COMPILER.
          default = (pkgs.mkShell.override { stdenv = tc.gcc; }) ({
            packages = [
              llvm.clang
              llvm.clang-tools
              pkgs.cmake
              pkgs.ninja
              pkgs.gtest
              pkgs.gbenchmark
              pkgs.gdb
              # fibers::tls (FIBERS_TLS): libssl / libcrypto, and the openssl
              # CLI that generates the test certificates and runs the interop
              # tests (s_client / s_server).
              pkgs.openssl
              pkgs.openssl.bin
              # External TLS test tools (GPL; invoked only, see
              # tests/tls/external/): testssl.sh, and tlsfuzzer's Python.
              pkgs.testssl
              # python-ecdsa is flagged in nixpkgs for its timing side
              # channel (CVE-2024-23342); irrelevant for a local test client,
              # so the flag is lifted for this one package only.
              (pkgs.python3.withPackages (ps: [
                (ps.ecdsa.overridePythonAttrs (old: {
                  meta = old.meta // { knownVulnerabilities = [ ]; };
                }))
              ]))
              pkgs.llhttp # fibers::http's parser
              pkgs.wrk # HTTP load generators (examples/README.md)
              pkgs.wrk2
            ];
            hardeningDisable = [ "fortify" ];
            FIBERS_TLSFUZZER = "${inputs.tlsfuzzer}";
            FIBERS_TLSLITE_NG = "${inputs.tlslite-ng}";
          } // twistDepsEnv pkgs);
        });
    };
}
