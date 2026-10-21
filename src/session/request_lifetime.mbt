///|
priv enum RequestLifetimeEvent {
  LocalFin
  PeerFin
  Cancel(UInt64)
  PeerReset(UInt64)
  PeerStop(UInt64)
}

///|
priv struct RequestLifetimeActions {
  abort_send : Bool
  stop_receiving : Bool
  notify_end : Bool
}

///|
priv struct RequestLifetime {
  mut local_send_closed : Bool
  mut peer_send_closed : Bool
  mut cancelled : Bool
  mut cancel_error_code : UInt64?
  mut end_notified : Bool
}

///|
fn RequestLifetime::new() -> RequestLifetime {
  {
    local_send_closed: false,
    peer_send_closed: false,
    cancelled: false,
    cancel_error_code: None,
    end_notified: false,
  }
}

///|
fn RequestLifetime::transition(
  self : RequestLifetime,
  event : RequestLifetimeEvent,
  stream_open : Bool,
) -> RequestLifetimeActions {
  let mut abort_send = false
  let mut stop_receiving = false
  let mut notify_end = false
  match event {
    LocalFin => self.local_send_closed = true
    PeerFin => self.peer_send_closed = true
    Cancel(error_code) => {
      self.cancelled = true
      if self.cancel_error_code is None {
        self.cancel_error_code = Some(error_code)
      }
      if stream_open {
        if !self.local_send_closed {
          self.local_send_closed = true
          abort_send = true
        }
        if !self.peer_send_closed {
          self.peer_send_closed = true
          stop_receiving = true
        }
        if !self.end_notified {
          self.end_notified = true
          notify_end = true
        }
      }
    }
    PeerReset(error_code) => {
      self.peer_send_closed = true
      self.cancelled = true
      if self.cancel_error_code is None {
        self.cancel_error_code = Some(error_code)
      }
      if !self.local_send_closed {
        self.local_send_closed = true
        abort_send = true
      }
      if !self.end_notified {
        self.end_notified = true
        notify_end = true
      }
    }
    PeerStop(error_code) => {
      self.local_send_closed = true
      self.cancelled = true
      if self.cancel_error_code is None {
        self.cancel_error_code = Some(error_code)
      }
      if !self.peer_send_closed {
        self.peer_send_closed = true
        stop_receiving = true
      }
      if !self.end_notified {
        self.end_notified = true
        notify_end = true
      }
    }
  }
  { abort_send, stop_receiving, notify_end, }
}
