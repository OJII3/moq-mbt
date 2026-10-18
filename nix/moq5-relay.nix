{ lib, stdenv, fetchFromGitHub, cmake, libmsquic }:

stdenv.mkDerivation {
  pname = "moq5-relay";
  version = "0-unstable-2026-10-18";

  src = fetchFromGitHub {
    owner = "openmoq";
    repo = "moq5";
    rev = "56dd3421ee95a4227c07c2091718d522fc8a5b25";
    hash = "sha256-KecwF130inqpC9N6nJwfU3zNZXpRZ/9/i46YE2LEZ3U=";
  };

  nativeBuildInputs = [ cmake ];
  buildInputs = [ libmsquic ];

  cmakeFlags = [
    "-DCMAKE_INSTALL_LIBDIR=lib"
    "-DCMAKE_INSTALL_INCLUDEDIR=include"
    "-Dmsquic_DIR=${libmsquic}/share/msquic"
    "-DMOQ_BUILD_RELAY=ON"
    "-DMOQ_BUILD_ADAPTER_MSQUIC=ON"
    "-DMOQ_BUILD_MSQUIC_MANAGED=ON"
    "-DMOQ_BUILD_TESTS=OFF"
    "-DMOQ_BUILD_EXAMPLES=OFF"
    "-DMOQ_BUILD_LOC=OFF"
    "-DMOQ_BUILD_CMAF=OFF"
    "-DMOQ_BUILD_CODEC_SIGNALING=OFF"
  ];

  meta = {
    description = "MoQ relay for Draft 18 interoperability checks";
    homepage = "https://github.com/openmoq/moq5";
    license = lib.licenses.asl20;
    platforms = lib.platforms.unix;
    mainProgram = "moq5-relay";
  };
}
