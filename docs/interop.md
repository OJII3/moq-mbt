# Draft 18 相互接続

## 固定 relay と CI の範囲

Nix flake は Draft 18 対応の moqtail source commit `4cdcb42de6a887754779d74a06c0bf4a8716e5cb` を固定し、Rust relay を package としてビルドする。CI とローカルの `scripts/check-interop.sh` / `scripts/check-moqtail-dual.sh` はこの同じ Nix package を使う。relay は子プロセスとして起動し、終了時に停止する。

以前の interop は Nix でビルドした moq-rs relay を使っていた。現在の開発環境、CI、手動起動スクリプトから moq-rs 依存を外し、過去の制約はこのファイルのシナリオ別結果に置き換えた。変更前の実装履歴は Git に残している。

CI の実 relay checks は Native QUIC の複数 Group/Object publish/subscribe、Browser WebTransport と raw QUIC の Draft 18 SETUP、証明書 pin の拒否、Native/Browser 双方向 publish/subscribe、Native `REQUEST_UPDATE` と独立 track の継続、Datagram / Subgroup 混在配送を確認する。

standalone FETCH (`MOQTAIL_RUN_FETCH=1`) は live PUBLISH 中に実行し、probe publisher が relay からの upstream FETCH request に応答して Group 0–1 を返す。Namespace withdrawal (`MOQTAIL_RUN_NAMESPACE=1`) も個別に有効化できる。joining FETCH (`MOQTAIL_RUN_JOINING_FETCH=1`) は固定 moqtail で現在失敗する。relay が FETCH objects を返す一方、probe は unsupported range の拒否を期待している。いずれも既定の relay interop では実行しない。

Playwright Chromium と WebTransport 実装も Nix flake で固定する。workflow は実行時にバージョンをログへ出す。一時 ECDSA P-256 証明書の正しい SHA-256 pin で接続し、誤った pin の拒否を確認する。Origin allowlist はこの relay 設定で有効にしていない。Origin 拒否は [#104](https://github.com/OJII3/moq-mbt/issues/104) で扱う。

仕様は [Draft 18](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.txt) に固定する。vi64 は QUIC の 62-bit varint と異なり、1〜9 bytes で UInt64 全域を表現する。SETUP の type 0x2f00 は制御ストリームの type も兼ねるので、別のストリームヘッダーを付けない。制御ストリームは SETUP の後も開いたままにする。

## ローカルで再現する

~~~sh
nix develop -c moon test
nix develop -c moon check --target js --deny-warn src/js_adapter
nix develop -c moon check --target wasm-gc --deny-warn src/protocol src/session
nix develop -c moon test --target wasm-gc src/protocol src/session
nix develop -c moon test --target js src/protocol src/session src/js_adapter
nix develop -c bash scripts/check-interop.sh
nix develop -c npm ci
nix develop -c bash scripts/check-moqtail-dual.sh
~~~

Native interop と dual transport のスクリプトはいずれも `moqtail` を 127.0.0.1:4443 で起動する。実行中の relay がある場合は終了してから実行する。ポートは `MOQTAIL_PORT` で変更できる。Native QUIC の証明書検証失敗を確認したあと、自己署名証明書の検証を loopback probe に限って無効にする。

手動確認では2つの terminal を使う。まず publisher を起動し、次に別の terminal から subscriber を起動する。両方に同じ relay、path、namespace、track を指定する。

~~~sh
nix develop -c moon run --release --target native src/cmd/track_probe -- \
  publish 127.0.0.1 4443 /interop interop multi-object 2 2 --insecure
~~~

~~~sh
nix develop -c moon run --release --target native src/cmd/track_probe -- \
  subscribe 127.0.0.1 4443 /interop interop multi-object 2 2 --insecure
~~~

`check-interop.sh` は一時証明書を生成し、moqtail relay を `127.0.0.1:4443` で起動して Native QUIC の Draft 18 interop を確認する。自己署名証明書が既定の Native 証明書検証で拒否されることを確認した後、loopback probe に限って `--insecure` を使う。終了時に publisher、relay、一時ファイルを削除する。

### Browser WebTransport 検証

`nix develop -c bash scripts/check-moqtail-dual.sh` は同じ moqtail relay process に raw QUIC と WebTransport の接続を同時に開き、両方で Draft 18 SETUP を確認する。さらに一時 ECDSA P-256 証明書の正しい SHA-256 pin で接続し、誤った pin の拒否と Native/Browser 双方向 publish/subscribe を確認する。各接続と E2E に期限を設定し、失敗時は relay / peer log を出力して子プロセスを終了する。Native probe は loopback の一時自己署名証明書に限り `--insecure` を使う。

Browser WebTransport の connect timeout、遅延した open/write、方向ごとの FIN、送信失敗、close 後に保留 I/O が解放されることは `src/js_adapter/webtransport_test.mbt` の fake-transport tests で決定的に確認する。`check-moqtail-dual.sh` は Datagram の単独 probe と、Datagram / Subgroup の混在配送を別々に確認する。Sans-I/O tests は peer-side decode と session routing も確認する。

ブラウザだけを手動確認する場合は `nix develop -c scripts/serve-moqtail.sh` を実行する。表示される URL と証明書 hash を使い、`WebTransport` の `protocols` に `moqt-18` を指定する。

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

この relay では WebTransport Origin allowlist を設定していない。Origin 拒否は [#104](https://github.com/OJII3/moq-mbt/issues/104) の対象とする。証明書 pin の拒否は自動確認する。接続成功はリスナー起動や TLS 成立だけでは判断せず、WebTransport `ready` の後に MoQ Draft 18 SETUP を受信することまで確認する。

CLI の末尾には Group 数と各 Group の Object 数を指定する。probe は `group-<group id>-object-<object id>` 形式の payload を生成し、subscriber 側で各 Group の Object ID 順と payload を照合する。SETUP はどちらのピアからも先に送れるため、probe は相手 SETUP の受信と、自分の SETUP の QUIC ACK の両方を待つ。続く PUBLISH_OK と SUBSCRIBE_OK も確認し、control request が relay を通ることを検証する。publisher は全 Subgroup stream の ACK 後に PUBLISH_DONE を送り、subscriber は各 Object stream の FIN、Object 数、Group 終了、PUBLISH_DONE の stream 数がそろった時点で成功する。

有限 batch の publisher は `PUBLISH_DONE(TRACK_ENDED)` で終了する。subscriber probe は購読 request stream を開いたまま応答と Object を受信し、`PUBLISH_DONE` 後に接続を閉じる。購読更新を検証する lifecycle probe も更新方向を開いたままにする。

publisher は PUBLISH_OK 後に 1 秒待ってから有限 batch を送る。この間に別プロセスの subscriber が relay に接続し、PUBLISH_DONE を含む完了通知を受け取れる。

`lifecycle`、Namespace、FETCH、Datagram probe は個別に実装されている。standalone / joining FETCH と Namespace withdrawal は、上記環境変数で追加確認できる。transport を介さない session state の検証には Sans-I/O tests を使う。

## 接続先と参照実装

~~~sh
nix develop -c moon run --release --target native src/cmd/setup_probe -- \
  relay.example.com 443 /moq
~~~

引数はホスト、ポート、MoQ URI の path と query。クライアントは ALPN moqt-18 だけを提示し、QUIC DATAGRAM の交渉を確認してから SETUP を送る。Native QUIC 用の PATH、AUTHORITY、MOQT_IMPLEMENTATION を含める。接続から相手の SETUP 受信までの期限は 10 秒。相手の SETUP 受信と自分の SETUP の ACK を待ってから接続を終了する。証明書検証は既定で有効。

Relay は [moqtail/moqtail](https://github.com/moqtail/moqtail) の Draft 18 branch commit `4cdcb42de6a887754779d74a06c0bf4a8716e5cb` を Nix flake で固定してビルドする。Native QUIC と WebTransport は同じ UDP port で ALPN により振り分けられる。

## API とリソースの範囲

ClientHandshake は I/O、時計、スレッドを持たない。バージョンは生成時に明示する。受信 SETUP の保持は最大 65,546 bytes に制限し、完了時に保持領域を解放する。エラー後は呼び出し元が対応する close code で接続を終了し、ハンドシェイクを破棄する。

MsQuic の callback は C のメモリだけを操作し、受信データをキューにコピーする。MoonBit の割り当ては呼び出し元がイベントを取り出す際に行う。stream 送信 buffer は `SEND_COMPLETE` まで保持し、そのイベントは QUIC ACK / cancel を示す。Datagram buffer は最終送信状態まで保持し、`DatagramSendStateChanged` は送信、喪失、ack、cancel を区別する。acknowledged は QUIC 層の確認であり、アプリケーションの配送確認ではない。受信イベントは 1 MiB / 256 件、stream 送信は 1 MiB / 256 件、Datagram 送信は 1 MiB / 64 件まで保持する。ストリーム上限は同時 16 本。上限超過を暗黙のデータ破棄として扱わない。

track_probe は一度に一つの track を publish または subscribe し、各 Group に一つの Subgroup stream を開く。session API は複数の track request と複数の Object stream を管理する。transport.Client の操作は単一の呼び出し元から直列に行い、終了時には close() を呼ぶ。自動 finalizer はない。Native executable は -lmsquic -lpthread を link する。
