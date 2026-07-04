{ lib
, rustPlatform
, pkg-config
, mpv-unwrapped
, makeWrapper
}:

# Plain Cargo workspace with a committed Cargo.lock -> buildRustPackage is the
# lower-footprint choice over crane. Only the `hypodj` daemon bin is installed;
# probe/play-probe are dev-only provers.
rustPlatform.buildRustPackage {
  pname = "hypodj";
  version = "0.1.0";

  src = lib.cleanSource ../.;

  cargoLock.lockFile = ../Cargo.lock;

  nativeBuildInputs = [ pkg-config makeWrapper ];
  # mpv.pc (for libmpv2-sys at build) + libmpv.so (DT_NEEDED at runtime).
  buildInputs = [ mpv-unwrapped ];

  # Install only the daemon. probe/play-probe are live-server provers, not
  # something a deployed service needs.
  cargoBuildFlags = [ "--bin" "hypodj" ];

  # Run only the cheap, offline hypodj-core tests (config parse + null-player
  # transitions). Scoping this keeps the check phase from compiling the
  # probe/play-probe bins (reqwest/rustls/aws-lc-rs) under the 2-core cap.
  doCheck = true;
  cargoTestFlags = [ "-p" "hypodj-core" ];

  # libmpv2-sys uses pkg-config to find mpv at build time.
  PKG_CONFIG_PATH = "${mpv-unwrapped.dev}/lib/pkgconfig";

  # LOAD-BEARING, not insurance: libmpv2-sys emits a bare
  # `cargo:rustc-link-lib=mpv` (hard DT_NEEDED libmpv.so.2) with no link-search,
  # so buildRustPackage does not reliably bake the RPATH. Without this wrap the
  # binary fails in ld.so at exec, BEFORE main() - the NullPlayer fallback (which
  # only covers an Mpv::init Err) never runs. LD_LIBRARY_PATH points at the dir
  # that actually contains libmpv.so.2.
  postInstall = ''
    wrapProgram $out/bin/hypodj \
      --prefix LD_LIBRARY_PATH : ${lib.makeLibraryPath [ mpv-unwrapped ]}
  '';

  meta = {
    description = "MPD-speaking OpenSubsonic client daemon (the DJ underneath)";
    mainProgram = "hypodj";
    license = with lib.licenses; [ mit asl20 ];
  };
}
