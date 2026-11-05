///|
/// A track subscription the application wants restored after reconnecting.
#derive(Debug, Eq)
pub(all) struct DesiredSubscription {
  track : @protocol.TrackName
  mut parameters : Array[@protocol.MessageParameter]
}

///|
pub extend DesiredSubscription with @moonbitlang/core/debug.Debug::{to_repr}

///|
pub extend DesiredSubscription with Eq::{equal, not_equal}

///|
fn copy_desired_subscription(
  desired : DesiredSubscription,
) -> DesiredSubscription {
  {
    track: {
      namespace_fields: desired.track.namespace_fields.copy(),
      name: desired.track.name,
    },
    parameters: desired.parameters.copy(),
  }
}

///|
fn copy_desired_subscriptions(
  desired_subscriptions : Array[DesiredSubscription],
) -> Array[DesiredSubscription] {
  desired_subscriptions.map(copy_desired_subscription)
}

///|
/// Return the current reconnect intent for this session. This snapshot can be
/// passed as `desired_subscriptions` to `ClientSession::new` when reconnecting.
pub fn ClientSession::desired_subscriptions(
  self : ClientSession,
) -> Array[DesiredSubscription] {
  copy_desired_subscriptions(self.desired_subscriptions)
}

///|
/// Remove all desired subscriptions for a track. This can be used while the
/// session is closed, when no request stream remains to cancel.
pub fn ClientSession::forget_desired_subscriptions(
  self : ClientSession,
  track : @protocol.TrackName,
) -> Unit {
  self.desired_subscriptions = self.desired_subscriptions.filter(fn(desired) {
    desired.track != track
  })
}

///|
fn ClientSession::remember_desired_subscription(
  self : ClientSession,
  track : @protocol.TrackName,
  parameters : Array[@protocol.MessageParameter],
) -> Unit {
  self.desired_subscriptions.push({ track, parameters: parameters.copy(), })
}

///|
fn ClientSession::forget_desired_subscription(
  self : ClientSession,
  track : @protocol.TrackName,
  parameters : Array[@protocol.MessageParameter],
) -> Unit {
  if self.desired_subscriptions.search_by(fn(desired) {
      desired.track == track && desired.parameters == parameters
    })
    is Some(index) {
    ignore(self.desired_subscriptions.remove(index))
  }
}

///|
fn ClientSession::update_desired_subscription_parameters(
  self : ClientSession,
  track : @protocol.TrackName,
  old_parameters : Array[@protocol.MessageParameter],
  new_parameters : Array[@protocol.MessageParameter],
) -> Unit {
  for desired in self.desired_subscriptions {
    if desired.track == track && desired.parameters == old_parameters {
      desired.parameters = new_parameters.copy()
      return
    }
  }
}

///|
fn ClientSession::resubscribe_desired(
  self : ClientSession,
) -> Result[Array[SessionAction], ClientSessionError] {
  let actions : Array[SessionAction] = []
  for desired in self.desired_subscriptions {
    match self.start_subscription_request(desired.track, desired.parameters) {
      Ok((_, request_actions)) => actions.append(request_actions)
      Err(error) => return Err(error)
    }
  }
  Ok(actions)
}
