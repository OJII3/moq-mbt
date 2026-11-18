///|
extern "C" fn exit(status : Int32) -> Unit = "exit"

///|
fn probe_usage() -> String {
  "usage: track_probe publish HOST PORT PATH NAMESPACE TRACK GROUPS OBJECTS_PER_GROUP [--datagrams] [--insecure] [--object-bytes=BYTES] [--hold-open] [--wait-for-start] [--wait-for-fetch] [--fetch-after-groups=N]\n       track_probe subscribe HOST PORT PATH NAMESPACE TRACK GROUPS OBJECTS_PER_GROUP [--datagrams] [--insecure] [--object-bytes=BYTES]\n       track_probe lifecycle HOST PORT PATH [--insecure]\n       track_probe namespace HOST PORT PATH [--insecure]\n       track_probe datagram-smoke HOST PORT PATH [--insecure]\n       track_probe fetch HOST PORT PATH NAMESPACE TRACK START_GROUP START_OBJECT END_GROUP END_OBJECT OBJECTS_PER_GROUP [--insecure]\n       track_probe late-join HOST PORT PATH NAMESPACE TRACK [--insecure]\n       track_probe joining-fetch HOST PORT PATH [--insecure]"
}

///|
fn main {
  let args = @env.args()
  if interop_client_mode(args) {
    run_interop_client(args)
    return
  }
  guard args.length() >= 2 else {
    eprintln(probe_usage())
    exit(2)
    return
  }
  let mut insecure = false
  let mut datagrams = false
  let mut hold_open = false
  let mut wait_for_start = false
  let mut wait_for_fetch = false
  let mut object_bytes_text : String? = None
  let mut fetch_after_groups_text : String? = None
  for arg in args {
    match arg {
      "--insecure" => insecure = true
      "--datagrams" => datagrams = true
      "--hold-open" => hold_open = true
      "--wait-for-start" => wait_for_start = true
      "--wait-for-fetch" => wait_for_fetch = true
      _ if arg.has_prefix("--object-bytes=") =>
        object_bytes_text = Some(arg[15:].to_owned())
      _ if arg.has_prefix("--fetch-after-groups=") =>
        fetch_after_groups_text = Some(arg[21:].to_owned())
      _ => ()
    }
  }
  let expected_args = match args[1] {
    "publish" =>
      9 +
      (if insecure { 1 } else { 0 }) +
      (if datagrams { 1 } else { 0 }) +
      (if hold_open { 1 } else { 0 }) +
      (if wait_for_start { 1 } else { 0 }) +
      (if wait_for_fetch { 1 } else { 0 }) +
      (if object_bytes_text is Some(_) { 1 } else { 0 }) +
      (if fetch_after_groups_text is Some(_) { 1 } else { 0 })
    "subscribe" =>
      9 +
      (if insecure { 1 } else { 0 }) +
      (if datagrams { 1 } else { 0 }) +
      (if object_bytes_text is Some(_) { 1 } else { 0 })
    "lifecycle" => if insecure { 6 } else { 5 }
    "namespace" => if insecure { 6 } else { 5 }
    "datagram-smoke" => if insecure { 6 } else { 5 }
    "fetch" => if insecure { 13 } else { 12 }
    "late-join" => if insecure { 8 } else { 7 }
    "joining-fetch" => if insecure { 6 } else { 5 }
    _ => 0
  }
  guard args.length() == expected_args else {
    eprintln(probe_usage())
    exit(2)
    return
  }
  try {
    let port = @string.parse_int(args[3])
    guard args[4].is_empty() ||
      args[4].has_prefix("/") ||
      args[4].has_prefix("?") else {
      fail("PATH must be empty or start with / or ?")
    }
    let object_bytes = match object_bytes_text {
      Some(text) => @string.parse_int(text)
      None => 0
    }
    guard object_bytes >= 0 else { fail("--object-bytes must not be negative") }
    let fetch_after_groups = match fetch_after_groups_text {
      Some(text) => @string.parse_int(text)
      None => 0
    }
    guard fetch_after_groups >= 0 else {
      fail("--fetch-after-groups must not be negative")
    }
    match args[1] {
      "lifecycle" => run_lifecycle(args[2], port, args[4], insecure)
      "namespace" => run_namespace(args[2], port, args[4], insecure)
      "datagram-smoke" => run_datagram_smoke(args[2], port, args[4], insecure)
      "joining-fetch" => run_joining_fetch(args[2], port, args[4], insecure)
      "late-join" =>
        run_late_join(args[2], port, args[4], args[5], args[6], insecure)
      "fetch" =>
        run_fetch(
          args[2],
          port,
          args[4],
          args[5],
          args[6],
          @string.parse_int(args[7]).to_uint64(),
          @string.parse_int(args[8]).to_uint64(),
          @string.parse_int(args[9]).to_uint64(),
          @string.parse_int(args[10]).to_uint64(),
          @string.parse_int(args[11]),
          insecure,
        )
      "publish" => {
        let group_count = @string.parse_int(args[7])
        let objects_per_group = @string.parse_int(args[8])
        guard group_count > 0 && objects_per_group > 0 else {
          fail("GROUPS and OBJECTS_PER_GROUP must be positive")
        }
        run_publisher(
          args[2],
          port,
          args[4],
          args[5],
          args[6],
          group_count,
          objects_per_group,
          object_bytes,
          datagrams,
          insecure,
          hold_open,
          wait_for_start,
          wait_for_fetch,
          fetch_after_groups,
        )
      }
      "subscribe" => {
        let group_count = @string.parse_int(args[7])
        let objects_per_group = @string.parse_int(args[8])
        guard group_count > 0 && objects_per_group > 0 else {
          fail("GROUPS and OBJECTS_PER_GROUP must be positive")
        }
        run_subscriber(
          args[2],
          port,
          args[4],
          args[5],
          args[6],
          group_count,
          objects_per_group,
          object_bytes,
          datagrams,
          insecure,
        )
      }
      _ => {
        eprintln(probe_usage())
        exit(2)
      }
    }
  } catch {
    error => {
      match error {
        @transport.TransportError(message) => eprintln(message)
        _ => eprintln(error.to_string())
      }
      exit(1)
    }
  }
}
