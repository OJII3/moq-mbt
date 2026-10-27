///|
/// Resolve the track a request was made for. Covers PUBLISH, SUBSCRIBE, and
/// FETCH requests so applications do not need to keep their own reverse map.
pub fn ClientSession::track_for_request(
  self : ClientSession,
  request_id : UInt64,
) -> @protocol.TrackName? {
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
  subscription_stream : Int64,
) -> @protocol.TrackName? {
  match self.inbound_subscriptions.get(subscription_stream) {
    Some(inbound) =>
      match self.requests.get(inbound.publisher_request_id) {
        Some(request) => Some(request.track)
        None => None
      }
    None =>
      match self.request_streams.get(subscription_stream) {
        Some(request_id) => self.track_for_request(request_id)
        None => None
      }
  }
}

///|
/// The Track Alias assigned to a local PUBLISH or SUBSCRIBE request.
pub fn ClientSession::track_alias_for_request(
  self : ClientSession,
  request_id : UInt64,
) -> UInt64? {
  match self.requests.get(request_id) {
    Some(request) => request.track_alias
    None => None
  }
}
