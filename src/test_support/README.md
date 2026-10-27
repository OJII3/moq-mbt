# Public session test support

Import `OJII3/moq-mbt/test_support`, `OJII3/moq-mbt/driver`, and
`OJII3/moq-mbt/session` in the consumer's test package. `ScriptedTransport`
implements the public `SessionTransport` contract without network I/O and works
with the real `SessionDriver` on both native and JavaScript targets.

```moonbit
let session = @session.ClientSession::new(
  @session.Draft18,
  { options: [] },
).unwrap()
let transport = @test_support.ScriptedTransport::new()
let driver = @driver.SessionDriver::new(transport, session, {
  max_in_flight_opens: 1,
  max_in_flight_sends: 1,
  max_in_flight_datagrams: 1,
})

// Exercise public session operations and dispatch their returned actions.
driver.dispatch(session.start().unwrap())
assert_eq(transport.take_calls(), [
  @test_support.OpenStream(@session.Unidirectional, @session.Control),
])
assert_true(transport.complete_open(@driver.Opened(10L)))
let setup_writes = transport.take_calls()
assert_true(setup_writes.length() > 0)
assert_true(transport.complete_send(10L, @driver.Written))

// Inject peer input, clock ticks, capability changes, or errors through the
// driver's existing public event API; inspect its notices and failure state.
driver.receive(@session.PeerClosed(0UL))
assert_eq(driver.take_notices(), [
  @session.SessionEnded(@session.Peer(0UL)),
])
```

`script_open`, `script_send`, and `script_datagram` append FIFO acceptance
results for the corresponding operation kind. Scripts are consumed per attempt,
including retries, and default to `Accepted` once exhausted. For example,
`transport.script_send(@driver.Retry)` refuses the next send; `driver.pump()`
retries it. `Fatal(status)` exercises the driver's transport failure path.

`take_calls()` drains all attempted transport operations, including refused
attempts, aborts, receive stops, and connection closes. It does not drain pending
completions. `complete_open(outcome)` and `complete_datagram(outcome)` complete
the oldest accepted operation of that kind. `complete_send(stream, outcome)`
completes the oldest accepted send on that stream. Each returns `false` when
there is no matching pending operation and removes the callback before invoking
it. Completion may immediately produce more calls through the driver's pump.

Abort and close calls are recorded without automatically settling pending
operations. Tests can explicitly complete those operations with cancellation,
reset, failure, or `Abandoned` outcomes from `driver`. Session parsing, event
handling, notices, and state transitions always belong to the existing session
and driver packages.
