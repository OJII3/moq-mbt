///|
/// Transport or peer event that changes a request lifecycle.
pub(all) enum RequestLifetimeEvent {
  LocalFin
  PeerFin
  Cancel(UInt64)
  PeerReset(UInt64)
  PeerStop(UInt64)
}

///|
/// Transport actions required by one lifecycle transition.
pub struct RequestLifetimeActions {
  abort_send : Bool
  stop_receiving : Bool
  notify_end : Bool
}

///|
/// Request lifecycle state without transport effects.
///
/// The fields remain immutable so the transition can be verified. Callers
/// replace their stored value with `RequestLifetimeUpdate::state` and then
/// read the actions from the same update.
pub struct RequestLifetime {
  local_send_closed : Bool
  peer_send_closed : Bool
  cancelled : Bool
  cancel_error_code : UInt64?
  end_notified : Bool
}

///|
/// Result of one lifecycle transition.
pub struct RequestLifetimeUpdate {
  state : RequestLifetime
  actions : RequestLifetimeActions
}

///|
#proof_pure
fn RequestLifetime::local_send_closed(self : RequestLifetime) -> Bool {
  self.local_send_closed
}

///|
#proof_pure
fn RequestLifetime::peer_send_closed(self : RequestLifetime) -> Bool {
  self.peer_send_closed
}

///|
#proof_pure
fn RequestLifetime::cancelled(self : RequestLifetime) -> Bool {
  self.cancelled
}

///|
#proof_pure
fn RequestLifetime::cancel_error_code(self : RequestLifetime) -> UInt64? {
  self.cancel_error_code
}

///|
#proof_pure
fn RequestLifetime::end_notified(self : RequestLifetime) -> Bool {
  self.end_notified
}

///|
#proof_pure
fn RequestLifetimeUpdate::state(
  self : RequestLifetimeUpdate,
) -> RequestLifetime {
  self.state
}

///|
#proof_pure
fn RequestLifetimeUpdate::actions(
  self : RequestLifetimeUpdate,
) -> RequestLifetimeActions {
  self.actions
}

///|
#proof_pure
fn RequestLifetimeActions::abort_send(self : RequestLifetimeActions) -> Bool {
  self.abort_send
}

///|
#proof_pure
fn RequestLifetimeActions::stop_receiving(
  self : RequestLifetimeActions,
) -> Bool {
  self.stop_receiving
}

///|
#proof_pure
fn RequestLifetimeActions::notify_end(self : RequestLifetimeActions) -> Bool {
  self.notify_end
}

///|
/// Create a lifecycle whose request stream is still open in both directions.
pub fn RequestLifetime::new() -> RequestLifetime where {
  proof_ensure: result => fresh(result) && lifetime_inv(result),
} {
  {
    local_send_closed: false,
    peer_send_closed: false,
    cancelled: false,
    cancel_error_code: None,
    end_notified: false,
  }
}

///|
/// Apply one event. The returned state replaces the previous state; the
/// returned actions describe the transport work required by the transition.
pub fn RequestLifetime::transition(
  self : RequestLifetime,
  event : RequestLifetimeEvent,
  stream_open : Bool,
) -> RequestLifetimeUpdate where {
  proof_require: lifetime_inv(self),
  proof_ensure: result => {
    step_post(self, event, result.state(), result.actions())
  },
} {
  let mut local_send_closed = self.local_send_closed
  let mut peer_send_closed = self.peer_send_closed
  let mut cancelled = self.cancelled
  let mut cancel_error_code = self.cancel_error_code
  let mut end_notified = self.end_notified
  let mut abort_send = false
  let mut stop_receiving = false
  let mut notify_end = false
  match event {
    LocalFin => local_send_closed = true
    PeerFin => peer_send_closed = true
    Cancel(error_code) => {
      cancelled = true
      match cancel_error_code {
        None => cancel_error_code = Some(error_code)
        Some(_) => ()
      }
      if stream_open {
        if !local_send_closed {
          local_send_closed = true
          abort_send = true
        }
        if !peer_send_closed {
          peer_send_closed = true
          stop_receiving = true
        }
        if !end_notified {
          end_notified = true
          notify_end = true
        }
      }
    }
    PeerReset(error_code) => {
      peer_send_closed = true
      cancelled = true
      match cancel_error_code {
        None => cancel_error_code = Some(error_code)
        Some(_) => ()
      }
      if !local_send_closed {
        local_send_closed = true
        abort_send = true
      }
      if !end_notified {
        end_notified = true
        notify_end = true
      }
    }
    PeerStop(error_code) => {
      local_send_closed = true
      cancelled = true
      match cancel_error_code {
        None => cancel_error_code = Some(error_code)
        Some(_) => ()
      }
      if !peer_send_closed {
        peer_send_closed = true
        stop_receiving = true
      }
      if !end_notified {
        end_notified = true
        notify_end = true
      }
    }
  }
  {
    state: {
      local_send_closed,
      peer_send_closed,
      cancelled,
      cancel_error_code,
      end_notified,
    },
    actions: { abort_send, stop_receiving, notify_end, },
  }
}

///|
/// Apply one event when the caller does not need the transport actions.
pub fn RequestLifetime::advance(
  self : RequestLifetime,
  event : RequestLifetimeEvent,
  stream_open : Bool,
) -> RequestLifetime {
  self.transition(event, stream_open).state
}

///|
/// Record that the application has been told this request ended.
pub fn RequestLifetime::with_end_notified(
  self : RequestLifetime,
) -> RequestLifetime {
  {
    local_send_closed: self.local_send_closed,
    peer_send_closed: self.peer_send_closed,
    cancelled: self.cancelled,
    cancel_error_code: self.cancel_error_code,
    end_notified: true,
  }
}

///|
/// Record that the local send direction is closed without a transport action.
pub fn RequestLifetime::with_local_send_closed(
  self : RequestLifetime,
) -> RequestLifetime {
  {
    local_send_closed: true,
    peer_send_closed: self.peer_send_closed,
    cancelled: self.cancelled,
    cancel_error_code: self.cancel_error_code,
    end_notified: self.end_notified,
  }
}

///|
/// Move directly to the terminal state used when both directions are closed
/// and the request has already been reported to the application.
pub fn RequestLifetime::terminated(self : RequestLifetime) -> RequestLifetime {
  {
    local_send_closed: true,
    peer_send_closed: true,
    cancelled: true,
    cancel_error_code: self.cancel_error_code,
    end_notified: true,
  }
}
