# 設計メモ

## 対象

まずは MoQ Transport Draft 18 を実装する。プロトコルのバージョンはトランスポートでの交渉結果からセッション作成時に明示し、セッション中は固定する。受信データからバージョンを推測しない。

## パッケージの役割

| パッケージ | 責務 |
| --- | --- |
| `protocol` | Draft 18 のメッセージ、wire format、エンコードとデコード |
| `session` | MoQ セッションの状態、制御メッセージ、受信イベントと送信アクション |
| `driver` | 送信アクションをトランスポートへ流す portable な pump と queue 管理 |
| `transport` | セッションと QUIC / WebTransport の接続、ストリームと接続のライフサイクル処理 |
| `test_support` | 公開 `SessionTransport` / `SessionDriver` を使う scripted transport |

依存の向きは `session` から `protocol`、`driver` から `session`、各トランスポートアダプターから `session` と `driver` とする。セッションは MsQuic や WebTransport の API に依存しない。

## セッションとトランスポートの境界

セッションはネットワーク I/O、スレッド、実時間時計を持たない Sans-I/O の状態機械とする。受信バイト列や datagram、ストリームの終了・リセット、明示的な時刻更新を入力として受け取り、送信アクションを返す。時刻は呼び出し元から渡す。

アダプターはセッションの送信アクションを QUIC / WebTransport の操作に変換し、トランスポートの受信イベントをセッションへ渡す。ストリーム参照はセッション内では不透明な値として扱い、実際のストリーム ID との対応はアダプターが管理する。

外部パッケージのテストは `test_support.ScriptedTransport` と実際の `SessionDriver` を組み合わせる。テスト側は受理・再試行・完了をscriptし、peerイベントをdriverへ渡す。sessionの処理や状態遷移をテスト用コードに複製しない。

送信アクションの実行は `driver` の `SessionDriver` に集約する。アダプターは `SessionTransport` の primitive 操作だけを実装し、`SessionAction` の variant ごとの分岐や queue を持たない。`SessionDriver` はアクションを受理されるまで保持し、`Retry` では同じアクションを再試行する。ストリームごとのアクション順を維持し、あるストリームの拒否が無関係なストリームの送信を止めない。queue 上限（open / send / datagram）は `SessionDriverConfig` としてアダプターから注入し、driver や adapter に固定値を埋め込まない。

driver は `Accepted` の完了を `StreamOpened`、`SendCompleted`、`StreamReset`、`TransportFailed` などのセッションイベントに変換して session へ戻す。`SendCompleted` は `Sent` または `Reset` を取り、bool の意味を呼び出し側に推測させない。開設や送信の致命的失敗は `TransportFailed` として渡し、アクションを黙って破棄しない。アクションの受理はトランスポートへの投入を意味し、ピアでのアプリケーション処理完了は別途通知で追跡する。

`AbortSend` を受けた driver は、そのストリームに残っている未送信アクションを破棄する。session は `AbortSend` を返した時点でそのストリームの保留送信数を閉じるため、GOAWAY の drain は abort した Object stream を待たない。abort 後に遅れて届いた `SendCompleted` は無視してよい。この契約により、アプリケーションが `SendCompleted` を注入して drain を補う必要はない。

受信側のバッファ上限、データのコピーまたは保持期間も API ごとに明示する。

## ClientSession の状態所有

`ClientSession` をプロトコル操作の公開 API とし、Request ID、Track Alias、リクエストの応答・終了状態、購読、および Object stream の状態を所有させる。リクエストストリームの受信バッファと応答デコードは PUBLISH / SUBSCRIBE で同じ経路を使い、受理状態や Alias の正本を一つにする。Alias からリクエストへの検索表は、この状態を参照するための索引として扱う。

要求の受理段階（pending / accepted / terminal）は `ClientRequest` が所有し、`RequestLifetime` の送受信方向の終了とは別の状態として扱う。terminal でも `PUBLISH_DONE` 後の Object drain が残るため、stream 数・稼働数・保留中の完了通知と drain 完了条件は `ObjectDrain` に集約する。Datagram の配送可否も同じ `ClientRequest` の状態判断を参照する。

Subgroup の reader / writer と送信完了待ちの状態は private なストリーム状態として管理し、処理を責務ごとのファイルへ分割する。Publisher / Subscriber の利便 API が必要になった場合は、`ClientSession` の操作と通知を使う上位のラッパーとして構築する。

track 名の逆引きは session が所有する。`track_for_request`、`subscription_track`、`track_alias_for_request` を公開し、`PublishAccepted`、`SubscriptionEnded`、`ObjectReceived`、`ObjectDatagramReceived` の notice には track を載せ、`PublishAccepted` は alias も返す。アプリケーションは track 名だけを目的とした対応表を持たない。

Object の送信も session 側の状態を使う。`send_object_datagram_for_track` は publisher 側の track から forwarding 中の subscription を列挙し、subscription ごとの Track Alias に詰め替えて datagram を返す。`ObjectGroupWriter` は `open_object_group` が返す token ベースの thin な handle で、Object ID の採番、stream 開設待ちのキュー、`rotate`、FIN 待ち、abort を session の状態だけで説明する。128 objects ごとの rotation のような方針はアプリケーション側に残す。

Object Datagram の送信は token で追跡する。`send_object_datagram` は `(token, actions)` を返し、`send_object_datagram_for_track` は fan-out した datagram ごとの token を返す。session は投入時に `DatagramQueued(token)`、transport がローカル書き込みに失敗した時に `DatagramLocalWriteFailed(token, status)` を通知する。`DatagramQueued` はローカル transport への投入であり、ピアへの到達や QUIC ACK を意味しない。送信可能な最大サイズは `max_datagram_length()` で問い合わせる。

## 再接続時の購読復元

`ClientSession` はアプリケーションが作成した SUBSCRIBE を desired subscription として保持する。接続が切れたら `desired_subscriptions()` で状態を取り出し、再接続用の `ClientSession::new_with_desired_subscriptions` に渡す。新sessionはSETUP完了後に購読を再発行し、新しいrequest IDを割り当てる。個別のdesiredを終了するには `cancel_request` を使い、切断中にtrackのdesiredを外すには `forget_desired_subscriptions` を使う。

ネットワーク接続の再試行や待ち時間は引き続きアダプターまたはアプリケーションが決める。sessionは再接続後の購読状態とrequest IDの再作成を担当する。

`ClientSessionError::message()` と `Show` はユーザー表示用の説明を返す。`close_code()` は接続終了コードを持つHandshakeErrorの場合に限って値を返す。その他のsession errorはローカルAPIの誤用やstream単位の失敗でも発生するため、共通の接続終了コードへ変換しない。

## Request stream lifetime

local PUBLISH、local SUBSCRIBE、peer SUBSCRIBE は同じ `RequestLifetime` で local send と peer send の終了を記録する。FIN は peer send 方向だけを閉じ、request の取消や Object stream の終了にはしない。RESET、STOP_SENDING、明示取消は共通遷移を通し、まだ開いている方向への transport action と `SubscriptionEnded` を一度だけ返す。`PUBLISH_DONE` の受信と Subgroup/Object stream の drain は request cancellation とは別に管理する。

`PUBLISH_DONE` と初回要求への `REQUEST_ERROR` を受信したら、local send 方向に一度だけ FIN を送る。`PUBLISH_DONE` 後も、通知された stream 数に達するまで遅着 Object を受信する。送信側は開設待ちの Object stream の callback も待ち、開設後に reset した空の stream も `PUBLISH_DONE` の stream 数に含める。

ローカル SUBSCRIBE で今後の更新が不要になった場合は、`finish_subscription_updates` で更新の送信方向だけを閉じる。以後の更新送信は拒否するが、相手の応答と Object の受信は継続する。

request stream の ID 索引は session の存続中 tombstone として保持する。終了後に遅れて届いた event を新しい peer request として誤配送しないため、解放は session が破棄される時とする。Object stream tombstone はその stream の FIN または RESET で解放する。通常の SUBSCRIBE Alias は、`PUBLISH_DONE` の stream 数と観測済み stream 数が一致し、active Object stream がなくなった時に解放する。cancel で `PUBLISH_DONE` を待てなくなった Alias は session 終了まで保持し、遅れて到着する Object stream を同じ request に結び付けて破棄できるようにする。

## M3 の Draft 18 スコープ

[M3](https://github.com/OJII3/moq-mbt/milestone/3) では `REQUEST_UPDATE` とその応答、リクエストストリームのキャンセルによる購読解除、`GOAWAY` とセッション終了、および Datagram Object の送受信を追加する。Datagram と Subgroup stream は同じ購読・Alias の状態を参照する。

購読フィルターは stream と Datagram の Object 送信に共通して適用する。相対フィルターの開始位置は初回要求の受理時とフィルターを明示した更新時に確定し、他のパラメーターだけの更新では維持する。`NEW_GROUP_REQUEST` の送信には、相手の `SUBSCRIBE_OK` に `DYNAMIC_GROUPS=1` が必要となる。

Draft 18 では `MAX_REQUEST_ID` / `REQUESTS_BLOCKED` と独立したキャンセルメッセージが削除されている。購読更新には `REQUEST_UPDATE`、キャンセルには開いているストリーム方向への `STOP_SENDING` / reset を使う。仕様の [§3.3.2](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-3.3.2)、[§10.9](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-10.9)、[Appendix A.2](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#appendix-A.2) を参照する。

`scripts/check-interop.sh` は moqtail relay に対する Draft 18 SETUP と複数 Group/Object の publish/subscribe、Native QUIC の証明書検証を確認する。`scripts/check-moqtail-dual.sh` は同じ moqtail endpoint に raw QUIC と WebTransport のクライアントを同時接続し、両方の SETUP、証明書 pin、Node 接続、Native/Browser 双方向 publish/subscribe を確認する。

この E2E 経路は Browser WebTransport ↔ moqtail relay ↔ Native raw QUIC である。Native WebTransport はこの経路に含まず、M6 の実装対象として扱う。Origin allowlist / 拒否は #92 の対象外で [#104](https://github.com/OJII3/moq-mbt/issues/104) で扱う。FETCH、Namespace withdrawal などの追加シナリオは [interop notes](interop.md) のコマンドで個別に確認する。

## 追加する構成

```text
src/
├── protocol/
├── session/
├── driver/
├── js_adapter/
└── transport/
```

MsQuic アダプターと JavaScript WebTransport アダプターを用意している。アダプターはそれぞれ `transport` と `js_adapter` にあり、`session` の送受信アクションとトランスポートのイベントを変換する。送信アクションの queue と再試行は `driver` が担う。Browser E2E probe は moqtail を介した Native/Browser 双方向 publish/subscribe を検証する。relay interop の追加シナリオは [interop notes](interop.md) に記録する。
