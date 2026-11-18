name = "OJII3/moq-mbt"

version = "0.4.4"

readme = "README.md"

repository = "https://github.com/OJII3/moq-mbt"

license = "MIT"

keywords = [ "moq", "quic", "webtransport", "media" ]

description = "Media over QUIC (MoQ) Transport implementation in MoonBit"

source = "src"

preferred_target = "native"

// Keep every compiler warning enabled and fix warnings in code. The proof-only
// loop lints stay off because only src/lifecycle is proof-enabled and its
// verified transitions contain no proof-relevant loops.

warnings = "+all-missing_invariant-missing_reasoning"

import {
  "mizchi/js_core@0.13.0",
  "mizchi/js_web@0.13.0",
  "mizchi/js_builtin@0.13.0",
}
