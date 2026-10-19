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

送信が詰まった場合の保持と再試行、受信側のバッファ上限、データのコピーまたは保持期間を API ごとに明示する。バックプレッシャーによるデータの破棄を暗黙に行わない。

## ClientSession の状態所有

`ClientSession` をプロトコル操作の公開 API とし、Request ID、Track Alias、リクエストの応答・終了状態、購読、および Object stream の状態を所有させる。リクエストストリームの受信バッファと応答デコードは PUBLISH / SUBSCRIBE で同じ経路を使い、受理状態や Alias の正本を一つにする。Alias からリクエストへの検索表は、この状態を参照するための索引として扱う。

Subgroup の reader / writer と送信完了待ちの状態は private なストリーム状態として管理し、処理を責務ごとのファイルへ分割する。Publisher / Subscriber の利便 API が必要になった場合は、`ClientSession` の操作と通知を使う上位のラッパーとして構築する。

## M3 の Draft 18 スコープ

[M3](https://github.com/OJII3/moq-mbt/milestone/3) では `REQUEST_UPDATE` とその応答、リクエストストリームのキャンセルによる購読解除、`GOAWAY` とセッション終了、および Datagram Object の送受信を追加する。Datagram と Subgroup stream は同じ購読・Alias の状態を参照する。

Draft 18 では `MAX_REQUEST_ID` / `REQUESTS_BLOCKED` と独立したキャンセルメッセージが削除されている。購読更新には `REQUEST_UPDATE`、キャンセルには開いているストリーム方向への `STOP_SENDING` / reset を使う。仕様の [§3.3.2](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-3.3.2)、[§10.9](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-10.9)、[Appendix A.2](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#appendix-A.2) を参照する。

## 追加する構成

```text
src/
├── protocol/
├── session/
└── transport/
```

MsQuic と WebTransport のアダプター、アプリ向けの高水準 API、インメモリシミュレーターは、実装上の必要が出た段階で追加する。セッションのシナリオは、ネットワークなしで再現できる形を目指す。
