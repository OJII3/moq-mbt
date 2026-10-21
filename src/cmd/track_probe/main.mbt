///|
extern "C" fn exit(status : Int32) -> Unit = "exit"

///|
fn main {
  let args = @env.args()
  guard args.length() >= 2 else {
    eprintln(
      "usage: track_probe publish HOST PORT PATH NAMESPACE TRACK GROUPS OBJECTS_PER_GROUP [--datagrams] [--insecure]\n       track_probe subscribe HOST PORT PATH NAMESPACE TRACK GROUPS OBJECTS_PER_GROUP [--datagrams] [--insecure]\n       track_probe datagram-smoke HOST PORT PATH [--insecure]",
    )
    exit(2)
    return
  }
  let insecure = args[args.length() - 1] == "--insecure"
  let datagrams = args[args.length() - 1] == "--datagrams" ||
    (insecure && args[args.length() - 2] == "--datagrams")
  let expected_args = match args[1] {
    "publish" =>
      9 + (if insecure { 1 } else { 0 }) + (if datagrams { 1 } else { 0 })
    "subscribe" =>
      9 + (if insecure { 1 } else { 0 }) + (if datagrams { 1 } else { 0 })
    "datagram-smoke" => if insecure { 6 } else { 5 }
    _ => 0
  }
  guard args.length() == expected_args else {
    eprintln(
      "usage: track_probe publish HOST PORT PATH NAMESPACE TRACK GROUPS OBJECTS_PER_GROUP [--datagrams] [--insecure]\n       track_probe subscribe HOST PORT PATH NAMESPACE TRACK GROUPS OBJECTS_PER_GROUP [--datagrams] [--insecure]\n       track_probe datagram-smoke HOST PORT PATH [--insecure]",
    )
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
    match args[1] {
      "datagram-smoke" => run_datagram_smoke(args[2], port, args[4], insecure)
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
          datagrams,
          insecure,
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
          datagrams,
          insecure,
        )
      }
      _ => {
        eprintln(
          "usage: track_probe publish HOST PORT PATH NAMESPACE TRACK GROUPS OBJECTS_PER_GROUP [--datagrams] [--insecure]\n       track_probe subscribe HOST PORT PATH NAMESPACE TRACK GROUPS OBJECTS_PER_GROUP [--datagrams] [--insecure]\n       track_probe datagram-smoke HOST PORT PATH [--insecure]",
        )
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
