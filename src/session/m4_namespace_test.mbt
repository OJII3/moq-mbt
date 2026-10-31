///|
test "namespace subscription enumerates previously published application namespaces" {
  let publisher = client_session_ready()
  let prefix = test_namespace([b"root"])
  let ns = test_namespace([b"root", b"child"])
  let outside = test_namespace([b"other", b"child"])
  ignore(publisher.publish_application_namespace(ns).unwrap())
  ignore(publisher.publish_application_namespace(ns).unwrap())
  ignore(publisher.publish_application_namespace(outside).unwrap())

  ignore(
    publisher.receive(@session.InboundStreamStarted(40, @session.Bidirectional)),
  )
  ignore(
    publisher.receive(
      @session.StreamData(
        40,
        @protocol.encode_subscribe_namespace({
          request_id: 1UL,
          track_namespace: prefix,
          parameters: [],
        }).unwrap(),
      ),
    ),
  )

  assert_eq(
    publisher.accept_namespace_subscription(@session.StreamId(40), []).unwrap(),
    [
      @session.SendChunk(
        40,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          @protocol.Namespace,
        ).unwrap(),
      ),
      @session.EmitNotice(@session.NamespaceSubscriptionEstablished(1UL, 40)),
      @session.SendChunk(
        40,
        @protocol.encode_namespace({
          track_namespace: test_namespace([b"child"]),
        }).unwrap(),
      ),
      @session.EmitNotice(@session.NamespaceDiscovered(1UL, ns)),
    ],
  )
}
