# Draft 18 相互接続

Native QUIC と WebTransport の両方で Draft 18 の SETUP を確認する。Native QUIC の interop script は複数 Group/Object の publish/subscribe と `PUBLISH_DONE` を検証する。Protocol state の詳細な REQUEST_UPDATE、FETCH、Namespace、Datagram シナリオは個別 probe と Sans-I/O tests で確認する。

Datagram probe は stream Object と同じ track 上で Object Datagram を送受信し、Alias、payload、通常 Object の Group 終端、`EndOfGroup`、`EndOfTrack` status を検証する。通常 payload のサンプルを少なくとも1つ受信したことを確認する。Datagram の追加マーカーは受信したものを検証するが、全件受信や順序は成功条件にしない。Datagram は Subgroup stream と混在させ、`PUBLISH_DONE` の stream 数には加えない。

moq-rs は1つの QUIC endpoint で raw QUIC と WebTransport を受け付ける。現在の CLI に Origin allowlist がないため、ブラウザ WebTransport の Origin 拒否は保証しない。証明書 pin の不一致は dual transport probe で確認する。

仕様は [Draft 18](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.txt) に固定する。vi64 は QUIC の 62-bit varint と異なり、1〜9 bytes で UInt64 全域を表現する。SETUP の type 0x2f00 は制御ストリームの type も兼ねるので、別のストリームヘッダーを付けない。制御ストリームは SETUP の後も開いたままにする。

## ローカルで再現する

~~~sh
nix develop -c moon test
nix develop -c moon test --target wasm
nix develop -c moon test --target js
nix develop -c bash scripts/check-interop.sh
nix develop -c bash scripts/check-moq-rs-dual.sh
~~~

Native interop と dual transport のスクリプトはいずれも `moq-rs` を 127.0.0.1:4443 で起動する。実行中の relay がある場合は終了してから実行する。Native QUIC の証明書検証失敗を確認したあと、自己署名証明書の検証を loopback probe に限って無効にする。

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

Standalone FETCH probe は retained log から START_GROUP〜END_GROUP（両端を含む）の全 Object を取得し、順序、payload、Properties、FETCH_OK end location、data stream FIN を検証する。事前に同じ relay へ `interop/multi-object` track の group 0〜2 を publish しておく。

~~~sh
nix develop -c moon run --release --target native src/cmd/track_probe -- \
  fetch 127.0.0.1 4443 /moq interop multi-object 0 2 --insecure
~~~

Datagram smoke を個別に実行するには、moq-rs が 127.0.0.1:4443 で待ち受けている状態で次を実行する。

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

`check-interop.sh` は一時証明書を生成し、moq-rs relay を `127.0.0.1:4443` で起動して Native QUIC の Draft 18 interop を確認する。自己署名証明書が既定の Native 証明書検証で拒否されることを確認した後、loopback probe に限って `--insecure` を使う。終了時に publisher、relay、一時ファイルを削除する。

### Browser WebTransport 検証

`nix develop -c bash scripts/check-moq-rs-dual.sh` は同じ moq-rs relay process に raw QUIC と WebTransport の接続を同時に開き、両方で Draft 18 SETUP を確認する。moq-rs は1つの UDP endpoint で WebTransport と raw QUIC の ALPN を受け付ける。スクリプトは一時 ECDSA P-256 証明書を作り、WebTransport の正しい SHA-256 certificate hash で接続したあと、誤った hash が拒否されることも確認する。Native probe は loopback の一時自己署名証明書に限り `--insecure` を使う。

ブラウザだけを手動確認する場合は `nix develop -c scripts/serve-moq-rs.sh` を実行する。表示される URL と証明書 hash を使い、`WebTransport` の `protocols` に `moqt-18` を指定する。

自己署名証明書を使った試験では、ブラウザの WebTransport `serverCertificateHashes` に表示された SHA-256 hash を `Uint8Array` として渡す。これはローカル試験用の明示的な証明書 pin であり、通常の信頼設定や Origin 検証を無効にしない。

ブラウザページでは表示された hash を次のように `Uint8Array` にし、接続を開始する。

~~~js
const hash = Uint8Array.from(atob("BASE64_HASH_FROM_SCRIPT"), c => c.charCodeAt(0))
const transport = new WebTransport("https://127.0.0.1:4443/interop", {
  protocols: ["moqt-18"],
  serverCertificateHashes: [{ algorithm: "sha-256", value: hash }],
})
await transport.ready
~~~

moq-rs の現在の CLI には WebTransport Origin allowlist がないため、異なる Origin からの拒否は検証できず、この構成では保証しない。証明書 pin の拒否は自動確認する。接続成功はリスナー起動や TLS 成立だけでは判断せず、WebTransport `ready` の後に MoQ Draft 18 SETUP を受信することまで確認する。

CLI の末尾には Group 数と各 Group の Object 数を指定する。probe は `group-<group id>-object-<object id>` 形式の payload を生成し、subscriber 側で各 Group の Object ID 順と payload を照合する。SETUP はどちらのピアからも先に送れるため、probe は相手 SETUP の受信と、自分の SETUP の QUIC ACK の両方を待つ。続く PUBLISH_OK と SUBSCRIBE_OK も確認し、control request が relay を通ることを検証する。publisher は全 Subgroup stream の ACK 後に PUBLISH_DONE を送り、subscriber は Object 数、Group 終了、PUBLISH_DONE の stream 数がそろった時点で成功する。

有限 batch の publisher は `PUBLISH_DONE(TRACK_ENDED)` で終了する。通常の subscriber probe は更新を送らないため、`finish_subscription_updates` で SUBSCRIBE の送信方向を先に FIN する。応答と Object の受信は継続し、PUBLISH_DONE の受信条件は緩和しない。購読更新を検証する lifecycle probe は更新方向を開いたままにする。

publisher は PUBLISH_OK 後に 1 秒待ってから有限 batch を送る。この間に別プロセスの subscriber が relay に接続し、PUBLISH_DONE を含む完了通知を受け取れる。

`lifecycle` probe は publisher と subscriber の接続を一つのプロセスで駆動し、各操作を SUBSCRIBE_OK、REQUEST_OK、Group 完了、PUBLISH_DONE などの通知に合わせて進める。track A の解除では request stream に STOP_SENDING / reset を送り、MsQuic の stream shutdown 完了を確認してから track B の次の Group を送る。更新失敗や relay 発 GOAWAY の再現には時間待ちを使わず、Sans-I/O の session tests を使う。

## 接続先と参照実装

~~~sh
nix develop -c moon run --release --target native src/cmd/setup_probe -- \
  relay.example.com 443 /moq
~~~

引数はホスト、ポート、MoQ URI の path と query。クライアントは ALPN moqt-18 だけを提示し、QUIC DATAGRAM の交渉を確認してから SETUP を送る。Native QUIC 用の PATH、AUTHORITY、MOQT_IMPLEMENTATION を含める。接続から相手の SETUP 受信までの期限は 10 秒。相手の SETUP 受信と自分の SETUP の ACK を待ってから接続を終了する。証明書検証は既定で有効。

Relay は [cloudflare/moq-rs の固定コミット](https://github.com/cloudflare/moq-rs/tree/8779758892d921bcfccbb6e4d9319c149db28c91) を `nix/moq-rs-relay.nix` でビルドする。Native QUIC と WebTransport は同じ endpoint の ALPN で選択される。Nixpkgs の `moq` は別用途の Go モック生成ツールなので使用しない。

## API とリソースの範囲

ClientHandshake は I/O、時計、スレッドを持たない。バージョンは生成時に明示する。受信 SETUP の保持は最大 65,546 bytes に制限し、完了時に保持領域を解放する。エラー後は呼び出し元が対応する close code で接続を終了し、ハンドシェイクを破棄する。

MsQuic の callback は C のメモリだけを操作し、受信データをキューにコピーする。MoonBit の割り当ては呼び出し元がイベントを取り出す際に行う。stream 送信 buffer は `SEND_COMPLETE` まで保持し、そのイベントは QUIC ACK / cancel を示す。Datagram buffer は最終送信状態まで保持し、`DatagramSendStateChanged` は送信、喪失、ack、cancel を区別する。acknowledged は QUIC 層の確認であり、アプリケーションの配送確認ではない。受信イベントは 1 MiB / 256 件、stream 送信は 1 MiB / 256 件、Datagram 送信は 1 MiB / 64 件まで保持する。ストリーム上限は同時 16 本。上限超過を暗黙のデータ破棄として扱わない。

track_probe は一度に一つの track を publish または subscribe し、各 Group に一つの Subgroup stream を開く。session API は複数の track request と複数の Object stream を管理する。transport.Client の操作は単一の呼び出し元から直列に行い、終了時には close() を呼ぶ。自動 finalizer はない。Native executable は -lmsquic -lpthread を link する。
