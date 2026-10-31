///|
/// Resolve the track a request was made for. Covers PUBLISH, SUBSCRIBE, and
/// FETCH requests so applications do not need to keep their own reverse map.
pub fn ClientSession::track_for_request(
  self : ClientSession,
  request_id : RequestId,
) -> @protocol.TrackName? {
  let request_id = request_id.to_uint64()
  match self.requests.get(request_id) {
    Some(request) => Some(request.track)
    None =>
      match self.fetches.get(request_id) {
        Some(fetch) => Some(fetch.track)
        None => None
      }
  }
}

///|
/// Resolve the publisher-side track of an accepted subscription stream. Both
/// peer SUBSCRIBE requests and local PUBLISH requests are covered.
pub fn ClientSession::subscription_track(
  self : ClientSession,
  subscription_stream : StreamId,
) -> @protocol.TrackName? {
  let subscription_stream = subscription_stream.to_int64()
  match self.inbound_subscriptions.get(subscription_stream) {
    Some(inbound) =>
      match self.requests.get(inbound.publisher_request_id) {
        Some(request) => Some(request.track)
        None => None
      }
    None =>
      match self.request_streams.get(subscription_stream) {
        Some(request_id) => self.track_for_request(RequestId(request_id))
        None => None
      }
  }
}

///|
/// The Track Alias assigned to a local PUBLISH or SUBSCRIBE request.
pub fn ClientSession::track_alias_for_request(
  self : ClientSession,
  request_id : RequestId,
) -> TrackAlias? {
  match self.requests.get(request_id.to_uint64()) {
    Some(request) =>
      match request.track_alias {
        Some(value) => Some(TrackAlias(value))
        None => None
      }
    None => None
  }
}

///|
/// The largest encoded Object Datagram the transport currently accepts, or 0
/// when Datagram sending is unavailable. The size includes MoQ framing, not
/// only the application payload.
pub fn ClientSession::max_datagram_length(self : ClientSession) -> Int32 {
  self.max_datagram_length
}
