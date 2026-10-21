///|
#external
priv type RawClient

///|
#external
priv type RawStream

///|
#borrow(host, alpn)
extern "C" fn native_connect(
  host : Bytes,
  port : Int32,
  alpn : Bytes,
  alpn_len : Int32,
  insecure : Bool,
  max_queued_bytes : Int32,
  max_queued_events : Int32,
  max_streams : Int32,
  handshake_timeout_ms : Int32,
  idle_timeout_ms : Int32,
) -> RawClient = "moq_client_connect"

///|
extern "C" fn native_valid(client : RawClient) -> Bool = "moq_client_valid"

///|
extern "C" fn native_open_uni(client : RawClient) -> RawStream = "moq_client_open_uni"

///|
extern "C" fn native_open_bidi(client : RawClient) -> RawStream = "moq_client_open_bidi"

///|
extern "C" fn native_stream_id(client : RawClient, stream : RawStream) -> Int64 = "moq_client_stream_id"

///|
extern "C" fn native_stream_close(
  client : RawClient,
  stream : RawStream,
) -> Unit = "moq_client_stream_close"

///|
#borrow(data)
extern "C" fn native_send(
  client : RawClient,
  stream : RawStream,
  data : Bytes,
  length : Int32,
  fin : Bool,
) -> Int32 = "moq_client_send"

///|
extern "C" fn native_abort_send(
  client : RawClient,
  stream : RawStream,
  error_code : Int64,
) -> Int32 = "moq_client_abort_send"

///|
extern "C" fn native_stop_receiving(
  client : RawClient,
  stream : RawStream,
  error_code : Int64,
) -> Int32 = "moq_client_stop_receiving"

///|
#borrow(data)
extern "C" fn native_send_datagram(
  client : RawClient,
  data : Bytes,
  length : Int32,
  id : Int64,
) -> Int32 = "moq_client_send_datagram"

///|
extern "C" fn native_datagram_state(client : RawClient) -> Int64 = "moq_client_datagram_state"

///|
extern "C" fn native_pop(client : RawClient) -> Int32 = "moq_client_pop"

///|
extern "C" fn native_event_id(client : RawClient) -> Int64 = "moq_client_event_id"

///|
extern "C" fn native_event_code(client : RawClient) -> Int64 = "moq_client_event_code"

///|
extern "C" fn native_event_data(client : RawClient) -> Bytes = "moq_client_event_data"

///|
extern "C" fn native_event_stream(client : RawClient) -> RawStream = "moq_client_event_stream"

///|
extern "C" fn native_shutdown(client : RawClient, error_code : Int64) -> Unit = "moq_client_shutdown"

///|
extern "C" fn native_shutdown_complete(client : RawClient) -> Bool = "moq_client_shutdown_complete"

///|
extern "C" fn native_free(client : RawClient) -> Unit = "moq_client_free"

///|
extern "C" fn native_sleep_ms(ms : Int32) -> Unit = "moq_sleep_ms"
