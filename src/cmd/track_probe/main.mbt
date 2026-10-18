///|
extern "C" fn exit(status : Int32) -> Unit = "exit"

///|
fn main {
  let args = @env.args()
  guard args.length() >= 2 else {
    eprintln(
      "usage: track_probe publish HOST PORT PATH NAMESPACE TRACK OBJECT [--insecure]\n       track_probe subscribe HOST PORT PATH NAMESPACE TRACK [--insecure]",
    )
    exit(2)
    return
  }
  let insecure = args[args.length() - 1] == "--insecure"
  let expected_publisher_args = if insecure { 9 } else { 8 }
  let expected_subscriber_args = if insecure { 8 } else { 7 }
  try {
    let port = @string.parse_int(args[3])
    guard args[4].is_empty() ||
      args[4].has_prefix("/") ||
      args[4].has_prefix("?") else {
      fail("PATH must be empty or start with / or ?")
    }
    match args[1] {
      "publish" if args.length() == expected_publisher_args =>
        run_publisher(
          args[2],
          port,
          args[4],
          args[5],
          args[6],
          args[7],
          insecure,
        )
      "subscribe" if args.length() == expected_subscriber_args =>
        run_subscriber(args[2], port, args[4], args[5], args[6], insecure)
      _ => {
        eprintln(
          "usage: track_probe publish HOST PORT PATH NAMESPACE TRACK OBJECT [--insecure]\n       track_probe subscribe HOST PORT PATH NAMESPACE TRACK [--insecure]",
        )
        exit(2)
      }
    }
  } catch {
    error => {
      eprintln(error.to_string())
      exit(1)
    }
  }
}
