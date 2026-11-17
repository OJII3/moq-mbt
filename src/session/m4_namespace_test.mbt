///|
test "namespace subscription enumerates previously published application namespaces" {
  let publisher = client_session_ready()
  let prefix = test_namespace([b"root"])
  let ns = test_namespace([b"root", b"child"])
  let outside = test_namespace([b"other", b"child"])
  ignore(publisher.publish_application_namespace(ns).unwrap())
  ignore(publisher.publish_application_namespace(ns).unwrap())
  ignore(publisher.publish_application_namespace(outside).unwrap())

  ignore(publisher.receive(InboundStreamStarted(40, Bidirectional)))
  ignore(
    publisher.receive(
      StreamData(
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
    publisher.accept_namespace_subscription(StreamId(40), []).unwrap(),
    [
      SendChunk(
        40,
        @protocol.encode_request_ok_for(
          { parameters: [], properties: b"", },
          Namespace,
        ).unwrap(),
      ),
      EmitNotice(NamespaceSubscriptionEstablished(1UL, 40)),
      SendChunk(
        40,
        @protocol.encode_namespace({
          track_namespace: test_namespace([b"child"]),
        }).unwrap(),
      ),
      EmitNotice(NamespaceDiscovered(1UL, ns)),
    ],
  )
}
