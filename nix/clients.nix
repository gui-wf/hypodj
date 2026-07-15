{ lib
, rustPlatform
}:

# The HypoDJ clients: the `dj` jukebox CLI and the `dj-gui` interactive TUI.
# Both are pure MPD/TCP over the shared hypodj-client lib - they never link
# libmpv, so this derivation needs no pkg-config, no mpv, and no wrap (unlike
# the daemon in package.nix). `cargo build --bin dj --bin dj-gui` only compiles
# the dependency graph of those two bins (hypodj-client + hypodj-cli/-tui);
# hypodj-core/-daemon and libmpv2-sys are never touched.
rustPlatform.buildRustPackage {
  pname = "hypodj-clients";
  version = "0.1.0";

  src = lib.cleanSource ../.;

  cargoLock.lockFile = ../Cargo.lock;

  cargoBuildFlags = [ "--bin" "dj" "--bin" "dj-gui" ];

  # Test only the client crates (offline, no mpv, no network).
  doCheck = true;
  cargoTestFlags = [ "-p" "hypodj-client" "-p" "hypodj-cli" "-p" "dj-gui" ];

  meta = {
    description = "HypoDJ clients: the dj jukebox CLI + dj-gui interactive TUI";
    mainProgram = "dj";
    license = with lib.licenses; [ mit asl20 ];
  };
}
