# Draft 18 相互接続

Native QUIC 上で Draft 18 の SETUP、複数 Group/Object の publish/subscribe、`PUBLISH_DONE`、REQUEST_UPDATE、購読解除、Standalone FETCH、Object Datagram を確認する。各 Group の Subgroup stream では Object ID、payload、Properties、Group 終端、stream 数を検証する。Standalone FETCH では retained Object の範囲・順序・payload・Properties、FETCH_OK end location、end-of-track flag と FETCH data stream の FIN を確認する。Namespace probe は購読・公開・発見・撤回・取消を確認する。固定 moq5 は Namespace 購読取消後に subscriber session を error code 3 で閉じるため、取消後の独立 track 継続は Sans-I/O で確認し、実ネットワーク項目には含めない。Draft 18 Namespace prefix 更新も固定 moq5 ではサポートされず、Sans-I/O で検証する。Joining FETCH probe は LargestObject filter 付き SUBSCRIBE 後に相対 Joining FETCH を送り、固定 moq5 が `INVALID_RANGE` で拒否することを記録する。この relay 実行では FETCH から live への切替を検証できないため、Joining FETCH の範囲解決と重複しない受信は Sans-I/O で確認する。moq5 の lifecycle probe は2つの track を購読し、track A の `FORWARD=0/1` 更新応答、track A を再開後の Object 配送、track A の解除、解除後も続く track B の配送を確認する。FORWARD 停止中のデータ抑止や更新失敗後の `PUBLISH_DONE(UPDATE_FAILED)` は Sans-I/O テストで決定的に確認する。

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

Standalone FETCH probe は retained log から START_GROUP〜END_GROUP（両端を含む）の全 Object を取得し、順序、payload、Properties、FETCH_OK end location、data stream FIN を検証する。事前に同じ relay へ `interop/multi-object` track の group 0〜2 を publish しておく。

~~~sh
nix develop -c moon run --release --target native src/cmd/track_probe -- \
  fetch 127.0.0.1 4443 /moq interop multi-object 0 2 --insecure
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

疎通スクリプトは一時ディレクトリに証明書と設定を生成し、moq5 を Native listener `127.0.0.1:4443` と WebTransport listener `127.0.0.1:4444` の両方で versions: [18]、認証 allow_all で起動する。WebTransport は profile `current`、path `/moq`、Origin allowlist `https://localhost:8000` を使う。自己署名証明書が既定の Native 証明書検証で拒否されることを確認した後、Native loopback probe に限って `--insecure` を使う。終了時に publisher、relay、一時ファイルを削除する。Datagram の送信状態は MsQuic の状態イベントとして公開し、acknowledged は QUIC 層の確認を表す。アプリケーションの処理・配送確認として扱わない。

### Browser WebTransport 検証

`check-interop.sh` は Native listener と既存の MoQ 相互接続を検査する。ブラウザ確認用に `nix develop -c scripts/serve-moq5-dual.sh` を実行すると、両 listener を起動し WebTransport 接続先、許可 Origin、一時証明書の SHA-256 hash を表示する。開発用ページを既定 Origin `http://localhost:8000` で配信し、relay に `https://localhost:4444/moq` で接続する。localhost は secure context として扱われる。生成証明書には `DNS:localhost` と `IP:127.0.0.1` の SAN があり、WebTransport の `serverCertificateHashes` 用 hash はスクリプトが表示する。証明書検証と Origin allowlist は有効のままにする。

自己署名証明書を使った試験では、ブラウザの WebTransport `serverCertificateHashes` に表示された SHA-256 hash を `Uint8Array` として渡す。これはローカル試験用の明示的な証明書 pin であり、通常の信頼設定や Origin 検証を無効にしない。

ブラウザページでは表示された hash を次のように `Uint8Array` にし、接続を開始する。

~~~js
const hash = Uint8Array.from(atob("BASE64_HASH_FROM_SCRIPT"), c => c.charCodeAt(0))
const transport = new WebTransport("https://localhost:4444/moq", {
  serverCertificateHashes: [{ algorithm: "sha-256", value: hash }],
})
await transport.ready
~~~

拒否条件は次のように確認する。`MOQ5_WEBTRANSPORT_ORIGIN=http://localhost:8001` のように allowlist と異なる Origin でページを配信して HTTP 403 になること、SAN が一致しないか未信頼の証明書を hash なしで使って TLS/WebTransport readiness に到達しないことを確認する。成功接続ではまず `WebTransport.ready` を待ち、その後 MoQ Draft 18 SETUP と Object 送受信を確認する。単にリスナーが起動したことや TLS が成立したことを MoQ 接続成功とは扱わない。

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

リレーは [openmoq/moq5 の固定コミット](https://github.com/openmoq/moq5/tree/56dd3421ee95a4227c07c2091718d522fc8a5b25) を `nix/moq5-relay.nix` でビルドする。`nix/wtquic.nix` は moq5 固定コミットの `setup_wtquic_deps.sh` が指定する WTQuic revision を pin し、既存の libmsquic と同じ MsQuic provider でビルドする。`nix develop` には両方の relay backend が含まれる。Nixpkgs の moq は Go のモック生成ツールなので使用しない。moq5 の実行方法と設定は[同コミットの relay README](https://github.com/openmoq/moq5/blob/56dd3421ee95a4227c07c2091718d522fc8a5b25/tools/moq5-relay/README.md)を参照する。

## API とリソースの範囲

ClientHandshake は I/O、時計、スレッドを持たない。バージョンは生成時に明示する。受信 SETUP の保持は最大 65,546 bytes に制限し、完了時に保持領域を解放する。エラー後は呼び出し元が対応する close code で接続を終了し、ハンドシェイクを破棄する。

MsQuic の callback は C のメモリだけを操作し、受信データをキューにコピーする。MoonBit の割り当ては呼び出し元がイベントを取り出す際に行う。stream 送信 buffer は `SEND_COMPLETE` まで保持し、そのイベントは QUIC ACK / cancel を示す。Datagram buffer は最終送信状態まで保持し、`DatagramSendStateChanged` は送信、喪失、ack、cancel を区別する。acknowledged は QUIC 層の確認であり、アプリケーションの配送確認ではない。受信イベントは 1 MiB / 256 件、stream 送信は 1 MiB / 256 件、Datagram 送信は 1 MiB / 64 件まで保持する。ストリーム上限は同時 16 本。上限超過を暗黙のデータ破棄として扱わない。

track_probe は一度に一つの track を publish または subscribe し、各 Group に一つの Subgroup stream を開く。session API は複数の track request と複数の Object stream を管理する。transport.Client の操作は単一の呼び出し元から直列に行い、終了時には close() を呼ぶ。自動 finalizer はない。Native executable は -lmsquic -lpthread を link する。
