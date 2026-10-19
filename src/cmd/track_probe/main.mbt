///|
extern "C" fn exit(status : Int32) -> Unit = "exit"

///|
fn main {
  let args = @env.args()
  guard args.length() >= 2 else {
    eprintln(
      "usage: track_probe publish HOST PORT PATH NAMESPACE TRACK GROUPS OBJECTS_PER_GROUP [--insecure]\n       track_probe subscribe HOST PORT PATH NAMESPACE TRACK GROUPS OBJECTS_PER_GROUP [--insecure]",
    )
    exit(2)
    return
  }
  let insecure = args[args.length() - 1] == "--insecure"
  let expected_args = match args[1] {
    "publish" => if insecure { 10 } else { 9 }
    "subscribe" => if insecure { 10 } else { 9 }
    _ => 0
  }
  guard args.length() == expected_args else {
    eprintln(
      "usage: track_probe publish HOST PORT PATH NAMESPACE TRACK GROUPS OBJECTS_PER_GROUP [--insecure]\n       track_probe subscribe HOST PORT PATH NAMESPACE TRACK GROUPS OBJECTS_PER_GROUP [--insecure]",
    )
    exit(2)
    return
  }
  try {
    let port = @string.parse_int(args[3])
    let group_count = @string.parse_int(args[7])
    let objects_per_group = @string.parse_int(args[8])
    guard group_count > 0 && objects_per_group > 0 else {
      fail("GROUPS and OBJECTS_PER_GROUP must be positive")
    }
    guard args[4].is_empty() ||
      args[4].has_prefix("/") ||
      args[4].has_prefix("?") else {
      fail("PATH must be empty or start with / or ?")
    }
    match args[1] {
      "publish" =>
        run_publisher(
          args[2],
          port,
          args[4],
          args[5],
          args[6],
          group_count,
          objects_per_group,
          insecure,
        )
      "subscribe" =>
        run_subscriber(
          args[2],
          port,
          args[4],
          args[5],
          args[6],
          group_count,
          objects_per_group,
          insecure,
        )
      _ => {
        eprintln(
          "usage: track_probe publish HOST PORT PATH NAMESPACE TRACK GROUPS OBJECTS_PER_GROUP [--insecure]\n       track_probe subscribe HOST PORT PATH NAMESPACE TRACK GROUPS OBJECTS_PER_GROUP [--insecure]",
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
