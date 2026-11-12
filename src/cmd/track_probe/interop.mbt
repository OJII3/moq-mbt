///|
let interop_test_cases : Array[String] = [
  "setup-only", "announce-only", "publish-namespace-done", "subscribe-namespace-lifecycle",
  "publish-namespace-multi-publisher-retention", "subscribe-error", "rendezvous-timeout",
  "announce-subscribe", "subscribe-before-announce",
]

///|
priv struct InteropEndpoint {
  host : String
  port : Int32
  path : String
}

///|
priv struct InteropOptions {
  relay_url : String?
  test_case : String?
  insecure : Bool
  list : Bool
  help : Bool
}

///|
priv struct InteropContext {
  endpoint : InteropEndpoint
  insecure : Bool
  roles : Array[String]
}

///|
fn InteropContext::new(
  endpoint : InteropEndpoint,
  insecure : Bool,
) -> InteropContext {
  { endpoint, insecure, roles: [], }
}

///|
fn InteropContext::connect(
  self : InteropContext,
  role : String,
) -> LifecyclePeer raise {
  let peer = interop_peer(self.endpoint, self.insecure)
  self.roles.push(role)
  peer
}

///|
fn interop_client_mode(args : Array[String]) -> Bool {
  if args.length() > 1 {
    match args[1] {
      "--list"
      | "-l"
      | "-r"
      | "--relay"
      | "-t"
      | "--test"
      | "--tls-disable-verify"
      | "-v"
      | "--verbose"
      | "--help"
      | "-h" => return true
      _ => ()
    }
    if args[1].has_prefix("--relay=") || args[1].has_prefix("--test=") {
      return true
    }
  }
  args.length() == 1 &&
  (
    @env.get_env_var("RELAY_URL") is Some(_) ||
    @env.get_env_var("TESTCASE") is Some(_)
  )
}

///|
fn parse_interop_options(args : Array[String]) -> InteropOptions raise {
  let mut relay_url = @env.get_env_var("RELAY_URL")
  let mut test_case = @env.get_env_var("TESTCASE")
  let mut insecure = @env.get_env_var("TLS_DISABLE_VERIFY") == Some("1")
  let mut list = false
  let mut help = false
  let mut index = 1
  while index < args.length() {
    let arg = args[index]
    match arg {
      "--list" | "-l" => list = true
      "--tls-disable-verify" => insecure = true
      "--verbose" | "-v" => ()
      "--help" | "-h" => help = true
      "-r" | "--relay" => {
        index += 1
        guard index < args.length() else { fail("missing value for \{arg}") }
        relay_url = Some(args[index])
      }
      "-t" | "--test" => {
        index += 1
        guard index < args.length() else { fail("missing value for \{arg}") }
        test_case = Some(args[index])
      }
      _ if arg.has_prefix("--relay=") => relay_url = Some(arg[8:].to_owned())
      _ if arg.has_prefix("--test=") => test_case = Some(arg[7:].to_owned())
      _ => fail("unknown option: \{arg}")
    }
    index += 1
  }
  { relay_url, test_case, insecure, list, help, }
}

///|
fn parse_interop_endpoint(url : String) -> InteropEndpoint raise {
  guard url.has_prefix("moqt://") else {
    fail("the native interop client requires a moqt:// raw QUIC relay URL")
  }
  let pieces = url[7:].split("/").map(fn(piece) { piece.to_owned() }).collect()
  guard !pieces.is_empty() && !pieces[0].is_empty() else {
    fail("relay URL is missing its host")
  }
  let authority = pieces[0]
  let mut host = ""
  let mut port = 443
  if authority.has_prefix("[") {
    let ipv6_parts = authority[1:]
      .split("]:")
      .map(fn(part) { part.to_owned() })
      .collect()
    guard ipv6_parts.length() == 2 && !ipv6_parts[0].is_empty() else {
      fail("IPv6 relay URLs must include a port, for example moqt://[::1]:443")
    }
    host = ipv6_parts[0]
    port = @string.parse_int(ipv6_parts[1])
  } else {
    let host_parts = authority
      .split(":")
      .map(fn(part) { part.to_owned() })
      .collect()
    guard host_parts.length() <= 2 else {
      fail("IPv6 relay URLs must enclose the host in brackets")
    }
    host = host_parts[0]
    if host_parts.length() == 2 {
      port = @string.parse_int(host_parts[1])
    }
  }
  guard !host.is_empty() && port > 0 && port <= 65535 else {
    fail("relay URL has an invalid host or port")
  }
  let path = if pieces.length() == 1 { "" } else { "/" + pieces[1:].join("/") }
  { host, port, path, }
}

///|
fn interop_peer(
  endpoint : InteropEndpoint,
  insecure : Bool,
) -> LifecyclePeer raise {
  let (client, session) = setup_client(
    endpoint.host,
    endpoint.port,
    endpoint.path,
    insecure,
    timeout_ms=2000UL,
  )
  { client, session, driver: SessionActionDriver::new(), closed_streams: {}, }
}

///|
fn run_interop_test(test_case : String, context : InteropContext) -> Unit raise {
  match test_case {
    "setup-only" => interop_setup_only(context)
    "announce-only" => interop_announce_only(context)
    "publish-namespace-done" => interop_publish_namespace_done(context)
    "subscribe-namespace-lifecycle" =>
      interop_subscribe_namespace_lifecycle(context)
    "publish-namespace-multi-publisher-retention" =>
      interop_publish_namespace_multi_publisher_retention(context)
    "subscribe-error" => interop_subscribe_error(context)
    "rendezvous-timeout" => interop_rendezvous_timeout(context)
    "announce-subscribe" => interop_announce_subscribe(context)
    "subscribe-before-announce" => interop_subscribe_before_announce(context)
    _ => fail("unsupported test case")
  }
}

///|
fn interop_yaml_quote(value : String) -> String {
  let escaped : Array[Byte] = [b'"']
  for byte in @utf8.encode(value) {
    let code = byte.to_int()
    if code == 0x22 {
      escaped.push(b'\\')
      escaped.push(b'"')
    } else if code == 0x5c {
      escaped.push(b'\\')
      escaped.push(b'\\')
    } else if code == 0x0a {
      escaped.push(b'\\')
      escaped.push(b'n')
    } else if code == 0x0d {
      escaped.push(b'\\')
      escaped.push(b'r')
    } else if code == 0x09 {
      escaped.push(b'\\')
      escaped.push(b't')
    } else if code < 0x20 {
      escaped.push(b' ')
    } else {
      escaped.push(byte)
    }
  }
  escaped.push(b'"')
  @utf8.decode_lossy(Bytes::from_array(escaped))
}

///|
fn print_interop_tap_point(
  index : Int32,
  test_case : String,
  passed : Bool,
  skipped : Bool,
  duration_ms : UInt64,
  roles : Array[String],
  message : String,
) -> Unit {
  let status = if passed { "ok" } else { "not ok" }
  if skipped {
    println("\{status} \{index} - \{test_case} # SKIP unsupported")
    return
  }
  println("\{status} \{index} - \{test_case}")
  println("  ---")
  println("  duration_ms: \{duration_ms}")
  if !roles.is_empty() {
    println("  sessions:")
    for role in roles {
      println("    \{role}:")
      println("      moqt_version: \"moqt-18\"")
      println("      transport: \"quic\"")
    }
  }
  if !passed && !message.is_empty() {
    println("  message: \{interop_yaml_quote(message)}")
  }
  println("  ...")
}

///|
fn run_interop_client(args : Array[String]) -> Unit {
  let options = parse_interop_options(args) catch {
    error => {
      eprintln(error.to_string())
      exit(2)
      return
    }
  }
  if options.help {
    println(
      "usage: track_probe [--relay URL] [--test NAME] [--tls-disable-verify] [--list]",
    )
    return
  }
  if options.list {
    for test_case in interop_test_cases {
      println(test_case)
    }
    return
  }
  let relay_url = match options.relay_url {
    Some(url) => url
    None => {
      eprintln("RELAY_URL or --relay is required")
      exit(2)
      return
    }
  }
  let endpoint = parse_interop_endpoint(relay_url) catch {
    error => {
      eprintln(error.to_string())
      exit(2)
      return
    }
  }
  let test_cases = match options.test_case {
    Some(test_case) => [test_case]
    None => interop_test_cases
  }
  println("TAP version 14")
  println("# moq-mbt Draft 18 native client")
  println("# Relay: \{relay_url}")
  println("1..\{test_cases.length()}")
  let mut failures = 0
  let mut unsupported = false
  for (index, test_case) in test_cases.iter2() {
    if !interop_test_cases.contains(test_case) {
      print_interop_tap_point(index + 1, test_case, true, true, 0UL, [], "")
      unsupported = true
    } else {
      let context = InteropContext::new(endpoint, options.insecure)
      let started = @env.now()
      let mut message = ""
      let passed = try {
        run_interop_test(test_case, context)
        true
      } catch {
        error => {
          message = error.to_string()
          eprintln("\{test_case}: \{message}")
          false
        }
      }
      let duration_ms = @env.now() - started
      print_interop_tap_point(
        index + 1,
        test_case,
        passed,
        false,
        duration_ms,
        context.roles,
        message,
      )
      if !passed {
        failures += 1
      }
    }
  }
  if failures > 0 {
    exit(1)
  }
  if unsupported {
    exit(127)
  }
}
