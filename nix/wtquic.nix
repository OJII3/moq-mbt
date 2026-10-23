{ stdenv, fetchFromGitHub, cmake, libmsquic }:

stdenv.mkDerivation {
  pname = "wtquic";
  version = "0-unstable-2026-10-18";

  src = fetchFromGitHub {
    owner = "rwl4";
    repo = "wtquic";
    rev = "644644727ed7ba853c3f14450dd42a9e8a40d7e4";
    hash = "sha256-AUbVgV7fxoyKUAcA8h+0eV2vSsJWeJhECQnfN0D/Nys=";
  };

  nativeBuildInputs = [ cmake ];
  buildInputs = [ libmsquic ];

  cmakeFlags = [
    "-DCMAKE_INSTALL_LIBDIR=lib"
    "-DCMAKE_INSTALL_INCLUDEDIR=include"
    "-DWTQ_REQUIRE_MSQUIC=ON"
    "-DWTQ_BUILD_NETWORK=OFF"
    "-DWTQ_BUILD_TESTS=OFF"
    "-DWTQ_BUILD_SIM=OFF"
    "-DWTQ_BUILD_BENCHMARKS=OFF"
    "-DWTQ_BUILD_EXAMPLES=OFF"
    "-Dmsquic_DIR=${libmsquic}/share/msquic"
  ];
}
