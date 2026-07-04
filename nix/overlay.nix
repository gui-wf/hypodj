# The hypodj overlay: adds `pkgs.hypodj`. Shared by flake.overlays.default and
# imported automatically by the NixOS module so `services.hypodj.package`'s
# default (`pkgs.hypodj`) resolves without the user wiring the overlay by hand.
final: prev: {
  hypodj = final.callPackage ./package.nix { };
}
