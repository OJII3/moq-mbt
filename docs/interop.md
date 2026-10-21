# Draft 18 相互接続

Native QUIC 上で Draft 18 の SETUP、複数 Group/Object の publish/subscribe、`PUBLISH_DONE`、REQUEST_UPDATE、購読解除、Object Datagram を確認する。各 Group の Subgroup stream では Object ID、payload、Group 終端、stream 数を検証する。moq5 の lifecycle probe は2つの track を購読し、track A の `FORWARD=0/1` 更新応答、track A を再開後の Object 配送、track A の解除、解除後も続く track B の配送を確認する。FORWARD 停止中のデータ抑止や更新失敗後の `PUBLISH_DONE(UPDATE_FAILED)` は Sans-I/O テストで決定的に確認する。

Datagram probe は stream Object と同じ track 上で Object Datagram を送受信し、Alias、payload、通常 Object の Group 終端、`EndOfGroup`、`EndOfTrack` status を検証する。通常 payload のサンプルを少なくとも1つ受信したことを確認する。Datagram の追加マーカーは受信したものを検証するが、全件受信や順序は成功条件にしない。Datagram は Subgroup stream と混在させ、`PUBLISH_DONE` の stream 数には加えない。

`moq5-relay` の CLI では購読更新拒否や relay 発の GOAWAY を誘発できない。更新拒否と `REQUEST_ERROR` / `PUBLISH_DONE(UPDATE_FAILED)`、GOAWAY 受信後の新規要求抑止・既存処理の drain・close は既存の Sans-I/O session tests で確認する。moq-rs 経路は従来どおり stream の複数 Object / Group 回帰に使う。

仕様は [Draft 18](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.txt) に固定する。vi64 は QUIC の 62-bit varint と異なり、1〜9 bytes で UInt64 全域を表現する。SETUP の type 0x2f00 は制御ストリームの type も兼ねるので、別のストリームヘッダーを付けない。制御ストリームは SETUP の後も開いたままにする。

## ローカルで再現する

~~~sh
nix develop -c moon test
nix develop -c moon test --target wasm
nix develop -c moon test --target js
nix develop -c bash scripts/check-interop.sh
~~~

疎通スクリプトは 127.0.0.1:4443 を使う。実行中のリレーがある場合は終了してから実行する。証明書検証に失敗する接続を確認したあと、SETUP、track 登録、Object 配送、moq5 の更新・解除シナリオを確認する。Datagram は best effort として扱い、Normal payload のサンプル受信を確認する一方、追加 Datagram の全件受信や順序は要求しない。

~~~text
REQUEST_UPDATE FORWARD=0 accepted
Received track B group 0 while track A was paused
REQUEST_UPDATE FORWARD=1 accepted
Received track A group 0 after forwarding resumed
Track A subscription cancelled
Received track B group 1 after track A cancellation
Verified track B PUBLISH_DONE (2 object streams)
Lifecycle and independent-track scenario passed
~~~

Datagram smoke を個別に実行するには、moq5-relay が 127.0.0.1:4443 で待ち受けている状態で次を実行する。

~~~sh
nix develop -c moon run --release --target native src/cmd/track_probe -- \
  datagram-smoke 127.0.0.1 4443 /moq --insecure
~~~

手動確認では2つの terminal を使う。まず publisher を起動し、次に別の terminal から subscriber を起動する。両方に同じ relay、path、namespace、track を指定する。

~~~sh
nix develop -c moon run --release --target native src/cmd/track_probe -- \
  publish 127.0.0.1 4443 /moq interop multi-object 2 2 --insecure
~~~

~~~sh
nix develop -c moon run --release --target native src/cmd/track_probe -- \
  subscribe 127.0.0.1 4443 /moq interop multi-object 2 2 --insecure
~~~

疎通スクリプトは一時ディレクトリに証明書と設定を生成し、moq5 を versions: [18]、認証 allow_all で起動する。自己署名証明書が既定の証明書検証で拒否されることを確認した後、ループバック接続に限って --insecure を使う。終了時に publisher、relay、一時ファイルを削除する。Datagram の送信状態は MsQuic の状態イベントとして公開し、acknowledged は QUIC 層の確認を表す。アプリケーションの処理・配送確認として扱わない。

CLI の末尾には Group 数と各 Group の Object 数を指定する。probe は `group-<group id>-object-<object id>` 形式の payload を生成し、subscriber 側で各 Group の Object ID 順と payload を照合する。SETUP はどちらのピアからも先に送れるため、probe は相手 SETUP の受信と、自分の SETUP の QUIC ACK の両方を待つ。続く PUBLISH_OK と SUBSCRIBE_OK も確認し、control request が relay を通ることを検証する。publisher は全 Subgroup stream の ACK 後に PUBLISH_DONE を送り、subscriber は Object 数、Group 終了、PUBLISH_DONE の stream 数がそろった時点で成功する。

有限 batch の publisher は `PUBLISH_DONE(TRACK_ENDED)` で終了する。通常の subscriber probe は更新を送らないため、`finish_subscription_updates` で SUBSCRIBE の送信方向を先に FIN する。これにより固定 moq-rs が応答終了時に送る STOP_SENDING と PUBLISH_DONE の到着順による競合を避ける。応答と Object の受信は継続し、PUBLISH_DONE の受信条件は緩和しない。購読更新を検証する lifecycle probe は更新方向を開いたままにする。

publisher は PUBLISH_OK 後に 1 秒待ってから有限 batch を送る。この間に別プロセスの subscriber が relay に接続し、PUBLISH_DONE を含む完了通知を受け取れる。

`lifecycle` probe は publisher と subscriber の接続を一つのプロセスで駆動し、各操作を SUBSCRIBE_OK、REQUEST_OK、Group 完了、PUBLISH_DONE などの通知に合わせて進める。track A の解除では request stream に STOP_SENDING / reset を送り、MsQuic の stream shutdown 完了を確認してから track B の次の Group を送る。更新失敗や relay 発 GOAWAY の再現には時間待ちを使わず、Sans-I/O の session tests を使う。

## 接続先と参照実装

~~~sh
nix develop -c moon run --release --target native src/cmd/setup_probe -- \
  relay.example.com 443 /moq
~~~

引数はホスト、ポート、MoQ URI の path と query。クライアントは ALPN moqt-18 だけを提示し、QUIC DATAGRAM の交渉を確認してから SETUP を送る。Native QUIC 用の PATH、AUTHORITY、MOQT_IMPLEMENTATION を含める。接続から相手の SETUP 受信までの期限は 10 秒。相手の SETUP 受信と自分の SETUP の ACK を待ってから接続を終了する。証明書検証は既定で有効。

リレーは [openmoq/moq5 の固定コミット](https://github.com/openmoq/moq5/tree/56dd3421ee95a4227c07c2091718d522fc8a5b25) を nix/moq5-relay.nix でビルドし、既存の libmsquic を使う。Nixpkgs の moq は Go のモック生成ツールなので使用しない。moq5 の実行方法と設定は[同コミットの relay README](https://github.com/openmoq/moq5/blob/56dd3421ee95a4227c07c2091718d522fc8a5b25/tools/moq5-relay/README.md)を参照する。

## API とリソースの範囲

ClientHandshake は I/O、時計、スレッドを持たない。バージョンは生成時に明示する。受信 SETUP の保持は最大 65,546 bytes に制限し、完了時に保持領域を解放する。エラー後は呼び出し元が対応する close code で接続を終了し、ハンドシェイクを破棄する。

MsQuic の callback は C のメモリだけを操作し、受信データをキューにコピーする。MoonBit の割り当ては呼び出し元がイベントを取り出す際に行う。stream 送信 buffer は `SEND_COMPLETE` まで保持し、そのイベントは QUIC ACK / cancel を示す。Datagram buffer は最終送信状態まで保持し、`DatagramSendStateChanged` は送信、喪失、ack、cancel を区別する。acknowledged は QUIC 層の確認であり、アプリケーションの配送確認ではない。受信イベントは 1 MiB / 256 件、stream 送信は 1 MiB / 256 件、Datagram 送信は 1 MiB / 64 件まで保持する。ストリーム上限は同時 16 本。上限超過を暗黙のデータ破棄として扱わない。

track_probe は一度に一つの track を publish または subscribe し、各 Group に一つの Subgroup stream を開く。session API は複数の track request と複数の Object stream を管理する。transport.Client の操作は単一の呼び出し元から直列に行い、終了時には close() を呼ぶ。自動 finalizer はない。Native executable は -lmsquic -lpthread を link する。
