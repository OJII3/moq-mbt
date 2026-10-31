///|
let interop_test_cases : Array[String] = [
  "setup-only", "announce-only", "publish-namespace-done", "subscribe-error",
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
priv enum InteropResponse {
  NamespaceAccepted(UInt64)
  SubscribeRejected(UInt64)
}

///|
fn interop_client_mode(args : Array[String]) -> Bool {
  if args.length() > 1 {
    match args[1] {
      "--list"
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
      "--list" => list = true
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
  guard pieces.length() > 0 && !pieces[0].is_empty() else {
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
fn interop_response_seen(
  notices : Array[@session.SessionNotice],
  expected : InteropResponse,
) -> Bool raise {
  for notice in notices {
    match (notice, expected) {
      (@session.NamespacePublicationAccepted(id, _), NamespaceAccepted(wanted)) if id ==
        wanted => return true
      (
        @session.NamespacePublicationRejected(id, _, error),
        NamespaceAccepted(wanted),
      ) if id == wanted =>
        fail("PUBLISH_NAMESPACE rejected: \{error.to_repr()}")
      (@session.SubscribeRejected(id, _), SubscribeRejected(wanted)) if id ==
        wanted => return true
      (@session.SubscribeEstablished(id, _), SubscribeRejected(wanted)) if id ==
        wanted => fail("SUBSCRIBE unexpectedly succeeded")
      (@session.SessionEnded(end), _) =>
        fail("relay ended the session: \{end.to_repr()}")
      _ => ()
    }
  }
  false
}

///|
fn wait_interop_response(
  peer : LifecyclePeer,
  expected : InteropResponse,
  initial : Array[@session.SessionNotice],
  timeout_ms : UInt64,
) -> Unit raise {
  if interop_response_seen(initial, expected) {
    return
  }
  let deadline = @env.now() + timeout_ms
  while @env.now() < deadline {
    if interop_response_seen(lifecycle_poll(peer, 50), expected) {
      return
    }
  }
  fail("timed out waiting for the expected relay response")
}

///|
fn wait_interop_withdrawal(
  peer : LifecyclePeer,
  request_id : UInt64,
  request_stream : Int64,
  initial : Array[@session.SessionNotice],
) -> Unit raise {
  let mut withdrawn = false
  for notice in initial {
    match notice {
      @session.NamespaceWithdrawn(id, stream) if id == request_id &&
        stream == request_stream => withdrawn = true
      @session.NamespacePublicationRejected(id, _, error) if id == request_id =>
        fail("PUBLISH_NAMESPACE withdrawal failed: \{error.to_repr()}")
      _ => ()
    }
  }
  let deadline = @env.now() + 2000UL
  while @env.now() < deadline &&
        (!withdrawn || peer.closed_streams.get(request_stream) is None) {
    for notice in lifecycle_poll(peer, 50) {
      match notice {
        @session.NamespaceWithdrawn(id, stream) if id == request_id &&
          stream == request_stream => withdrawn = true
        @session.NamespacePublicationRejected(id, _, error) if id == request_id =>
          fail("PUBLISH_NAMESPACE withdrawal failed: \{error.to_repr()}")
        @session.SessionEnded(end) =>
          fail("relay ended the session during withdrawal: \{end.to_repr()}")
        _ => ()
      }
    }
  }
  guard withdrawn && peer.closed_streams.get(request_stream) is Some(_) else {
    fail("namespace request stream did not close cleanly")
  }
}

///|
fn run_interop_test(
  test_case : String,
  endpoint : InteropEndpoint,
  insecure : Bool,
) -> Unit raise {
  match test_case {
    "setup-only" => {
      let (client, _) = setup_client(
        endpoint.host,
        endpoint.port,
        endpoint.path,
        insecure,
        timeout_ms=2000UL,
      )
      defer client.close()
    }
    "announce-only" | "publish-namespace-done" => {
      let peer = interop_peer(endpoint, insecure)
      defer peer.client.close()
      let track_namespace : @protocol.TrackNamespace = {
        fields: [b"moq-test", b"interop"],
      }
      let (request_id, actions) = peer.session
        .publish_namespace(track_namespace, [])
        .unwrap()
      let request_id = request_id.to_uint64()
      let (notices, opened) = drive_session_actions(
        peer.client,
        peer.session,
        actions,
        peer.driver,
      )
      wait_interop_response(
        peer,
        NamespaceAccepted(request_id),
        notices,
        2000UL,
      )
      if test_case == "publish-namespace-done" {
        let request_stream = lifecycle_request_stream(opened, request_id)
        let (notices, _) = drive_session_result(
          peer.client,
          peer.session,
          peer.session.cancel_request(request_id),
          peer.driver,
        )
        wait_interop_withdrawal(peer, request_id, request_stream, notices)
      }
    }
    "subscribe-error" => {
      let peer = interop_peer(endpoint, insecure)
      defer peer.client.close()
      let track = @protocol.TrackName::from_utf8_fields(
        ["nonexistent", "namespace"],
        "test-track",
      )
      let (request_id, actions) = peer.session.subscribe_track(track).unwrap()
      let request_id = request_id.to_uint64()
      let (notices, _) = drive_session_actions(
        peer.client,
        peer.session,
        actions,
        peer.driver,
      )
      wait_interop_response(
        peer,
        SubscribeRejected(request_id),
        notices,
        2000UL,
      )
    }
    _ => fail("unsupported test case")
  }
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
      println("ok \{index + 1} - \{test_case} # SKIP unsupported")
      unsupported = true
    } else {
      let passed = try {
        run_interop_test(test_case, endpoint, options.insecure)
        true
      } catch {
        error => {
          eprintln("\{test_case}: \{error.to_string()}")
          false
        }
      }
      if passed {
        println("ok \{index + 1} - \{test_case}")
      } else {
        println("not ok \{index + 1} - \{test_case}")
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
