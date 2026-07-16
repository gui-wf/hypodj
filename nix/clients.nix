{ lib
, rustPlatform
, pkg-config
, mpv-unwrapped
, makeWrapper
# DISPLAY-only enrichment (from flake.nix): the semver read from Cargo.toml and
# the HYPODJ_BUILD_INFO grammar baked into the runtime env (the nix sandbox has
# no .git, so this is what `dj --version` / `dj-gui --version` shows).
, buildInfo ? ""
, cargoVersion ? "0.1.0"
}:

# The HypoDJ clients: the `dj` jukebox CLI and the `dj-gui` interactive TUI.
#
# The `cc` feature is now DEFAULT (a plain `cargo build` compiles the client-side
# Claude Code CLI backend), so both bins pull `hypodj-nl` -> `hypodj-core`. That
# transitively links libmpv (hypodj-core's real player), so - unlike the old
# thin-client build - this derivation DOES need mpv at build + link time. The owner
# deliberately dropped the thin-client "never link libmpv" invariant to make cc the
# default. `cargo build --bin dj --bin dj-gui` still only compiles the dependency
# graph of the two bins (now including hypodj-nl + hypodj-core, not hypodj-daemon).
#
# Runtime cc is gated on `claude` being on PATH (cc_available), so this package runs
# unchanged on a machine without it (falls back to the daemon `nl` path). The check
# phase is CERTLESS + network-less: cc_available() is false, so no live `claude`
# call ever runs - only the PURE cc unit tests (prompt build, envelope parse +
# off-surface rejection) execute.
rustPlatform.buildRustPackage {
  pname = "hypodj-clients";
  version = cargoVersion;

  src = lib.cleanSource ../.;

  cargoLock.lockFile = ../Cargo.lock;

  nativeBuildInputs = [ pkg-config makeWrapper ];
  # mpv.pc (for libmpv2-sys at build, pulled transitively via hypodj-core) +
  # libmpv.so (DT_NEEDED at link/runtime).
  buildInputs = [ mpv-unwrapped ];

  cargoBuildFlags = [ "--bin" "dj" "--bin" "dj-gui" ];

  # Test only the client crates (offline, no network). The cc tests are pure - a
  # missing `claude` just means cc_available() is false, never a live call.
  doCheck = true;
  cargoTestFlags = [ "-p" "hypodj-client" "-p" "hypodj-cli" "-p" "dj-gui" ];

  # libmpv2-sys uses pkg-config to find mpv at build time.
  PKG_CONFIG_PATH = "${mpv-unwrapped.dev}/lib/pkgconfig";

  # libmpv2-sys emits a bare `cargo:rustc-link-lib=mpv` (hard DT_NEEDED libmpv.so.2)
  # with no link-search, so the RPATH is not reliably baked. Wrap both bins so
  # ld.so finds libmpv.so.2 at exec (the clients never call the player, but the
  # hard DT_NEEDED must still resolve).
  # --set-default so an explicit runtime HYPODJ_BUILD_INFO still wins; the baked
  # value is what makes `--version` show the hash on a nix build (no .git here).
  postInstall = ''
    for b in dj dj-gui; do
      wrapProgram $out/bin/$b \
        --prefix LD_LIBRARY_PATH : ${lib.makeLibraryPath [ mpv-unwrapped ]} \
        --set-default HYPODJ_BUILD_INFO ${lib.escapeShellArg buildInfo}
    done
  '';

  meta = {
    description = "HypoDJ clients: the dj jukebox CLI + dj-gui interactive TUI";
    mainProgram = "dj";
    license = with lib.licenses; [ mit asl20 ];
  };
}
