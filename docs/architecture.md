# 設計メモ

## 対象

まずは MoQ Transport Draft 18 を実装する。プロトコルのバージョンはトランスポートでの交渉結果からセッション作成時に明示し、セッション中は固定する。受信データからバージョンを推測しない。

## パッケージの役割

| パッケージ | 責務 |
| --- | --- |
| `protocol` | Draft 18 のメッセージ、wire format、エンコードとデコード |
| `session` | MoQ セッションの状態、制御メッセージ、受信イベントと送信アクション |
| `transport` | セッションと QUIC / WebTransport の接続、ストリームと接続のライフサイクル処理 |

依存の向きは `session` から `protocol`、各トランスポートアダプターから `session` とする。セッションは MsQuic や WebTransport の API に依存しない。

## セッションとトランスポートの境界

セッションはネットワーク I/O、スレッド、実時間時計を持たない Sans-I/O の状態機械とする。受信バイト列や datagram、ストリームの終了・リセット、明示的な時刻更新を入力として受け取り、送信アクションを返す。時刻は呼び出し元から渡す。

アダプターはセッションの送信アクションを QUIC / WebTransport の操作に変換し、トランスポートの受信イベントをセッションへ渡す。ストリーム参照はセッション内では不透明な値として扱い、実際のストリーム ID との対応はアダプターが管理する。

送信アクションを受け取ったアダプターは、受理または致命的失敗の通知までそのアクションを保持する。ストリームごとのアクション順を維持し、一時的なバックプレッシャーでは同じアクションを再試行する。ストリーム開設や送信の致命的失敗は `TransportFailed` としてセッションへ渡し、アクションを黙って破棄しない。セッションがアクションを返した時点で内部状態を進めるため、この責務はアダプター側にある。アクションの受理はトランスポートへの投入を意味し、ピアでのアプリケーション処理完了は別途通知で追跡する。

受信側のバッファ上限、データのコピーまたは保持期間も API ごとに明示する。

## ClientSession の状態所有

`ClientSession` をプロトコル操作の公開 API とし、Request ID、Track Alias、リクエストの応答・終了状態、購読、および Object stream の状態を所有させる。リクエストストリームの受信バッファと応答デコードは PUBLISH / SUBSCRIBE で同じ経路を使い、受理状態や Alias の正本を一つにする。Alias からリクエストへの検索表は、この状態を参照するための索引として扱う。

要求の受理段階（pending / accepted / terminal）は `ClientRequest` が所有し、`RequestLifetime` の送受信方向の終了とは別の状態として扱う。terminal でも `PUBLISH_DONE` 後の Object drain が残るため、stream 数・稼働数・保留中の完了通知と drain 完了条件は `ObjectDrain` に集約する。Datagram の配送可否も同じ `ClientRequest` の状態判断を参照する。

Subgroup の reader / writer と送信完了待ちの状態は private なストリーム状態として管理し、処理を責務ごとのファイルへ分割する。Publisher / Subscriber の利便 API が必要になった場合は、`ClientSession` の操作と通知を使う上位のラッパーとして構築する。

## Request stream lifetime

local PUBLISH、local SUBSCRIBE、peer SUBSCRIBE は同じ `RequestLifetime` で local send と peer send の終了を記録する。FIN は peer send 方向だけを閉じ、request の取消や Object stream の終了にはしない。RESET、STOP_SENDING、明示取消は共通遷移を通し、まだ開いている方向への transport action と `SubscriptionEnded` を一度だけ返す。`PUBLISH_DONE` の受信と Subgroup/Object stream の drain は request cancellation とは別に管理する。

`PUBLISH_DONE` と初回要求への `REQUEST_ERROR` を受信したら、local send 方向に一度だけ FIN を送る。`PUBLISH_DONE` 後も、通知された stream 数に達するまで遅着 Object を受信する。送信側は開設待ちの Object stream の callback も待ち、開設後に reset した空の stream も `PUBLISH_DONE` の stream 数に含める。

ローカル SUBSCRIBE で今後の更新が不要になった場合は、`finish_subscription_updates` で更新の送信方向だけを閉じる。以後の更新送信は拒否するが、相手の応答と Object の受信は継続する。

request stream の ID 索引は session の存続中 tombstone として保持する。終了後に遅れて届いた event を新しい peer request として誤配送しないため、解放は session が破棄される時とする。Object stream tombstone はその stream の FIN または RESET で解放する。通常の SUBSCRIBE Alias は、`PUBLISH_DONE` の stream 数と観測済み stream 数が一致し、active Object stream がなくなった時に解放する。cancel で `PUBLISH_DONE` を待てなくなった Alias は session 終了まで保持し、遅れて到着する Object stream を同じ request に結び付けて破棄できるようにする。

## M3 の Draft 18 スコープ

[M3](https://github.com/OJII3/moq-mbt/milestone/3) では `REQUEST_UPDATE` とその応答、リクエストストリームのキャンセルによる購読解除、`GOAWAY` とセッション終了、および Datagram Object の送受信を追加する。Datagram と Subgroup stream は同じ購読・Alias の状態を参照する。

購読フィルターは stream と Datagram の Object 送信に共通して適用する。相対フィルターの開始位置は初回要求の受理時とフィルターを明示した更新時に確定し、他のパラメーターだけの更新では維持する。`NEW_GROUP_REQUEST` の送信には、相手の `SUBSCRIBE_OK` に `DYNAMIC_GROUPS=1` が必要となる。

Draft 18 では `MAX_REQUEST_ID` / `REQUESTS_BLOCKED` と独立したキャンセルメッセージが削除されている。購読更新には `REQUEST_UPDATE`、キャンセルには開いているストリーム方向への `STOP_SENDING` / reset を使う。仕様の [§3.3.2](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-3.3.2)、[§10.9](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-10.9)、[Appendix A.2](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#appendix-A.2) を参照する。

`scripts/check-interop.sh` は moq-rs relay に対する Draft 18 SETUP と複数 Group/Object の publish/subscribe、Native QUIC の証明書検証を確認する。`scripts/check-moq-rs-dual.sh` は同じ moq-rs endpoint に raw QUIC と WebTransport のクライアントを同時接続し、両方の SETUP を確認する。

## 追加する構成

```text
src/
├── protocol/
├── session/
├── js_adapter/
└── transport/
```

MsQuic アダプターと JavaScript WebTransport アダプターを用意している。アダプターはそれぞれ `transport` と `js_adapter` にあり、`session` の送受信アクションとトランスポートのイベントを変換する。現在のブラウザ interop check は moq-rs への WebTransport 接続と Draft 18 SETUP を確認する。Native と Web 間のアプリレベル publish/subscribe E2E は未検証である。セッションのシナリオはネットワークなしで再現できる形を目指す。
