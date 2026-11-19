# Draft 18 相互接続

## 固定 relay と CI の範囲

interop の relay は Draft 18 対応の aiomoqt を Docker image digest で固定する（既定値は `scripts/interop-relay.sh`）。CI とローカルの `scripts/check-interop.sh` / `scripts/check-interop-dual.sh` は同じ image を使う。relay は子プロセスとして起動し、終了時に停止する。Docker の UDP port mapping は loopback に限る。

`scripts/interop-relay.sh` が relay の起動と停止を担う。stream ring は 8 MiB に上げる。これは 2 MiB Object を一つの stream write で送るためであり、MoQ の Object 長の上限ではない。

CI の実 relay checks は Native QUIC の複数 Group/Object publish/subscribe、2 MiB Object の分割送信、同一 track への 3 subscriber fan-out、Browser WebTransport と raw QUIC の Draft 18 SETUP、証明書 pin の拒否、Native/Browser 双方向 publish/subscribe と 2 MiB Object、browser の Object Datagram 受信、Native `REQUEST_UPDATE` と独立 track の継続、Datagram / Subgroup 混在配送、standalone FETCH の group/object offset、late join を確認する。

固定 relay は次を実装する。

- 同じ namespace を広告する publisher が残っている間、namespace を保持する。
- upstream FETCH の group と object の範囲を保持する。
- `RENDEZVOUS_TIMEOUT` を解釈し、期限まで publisher を待つ。
- raw QUIC と WebTransport を同じ UDP port で ALPN により振り分ける。

このため `publish-namespace-multi-publisher-retention` と `rendezvous-timeout` を含む 10 個の moq-interop-runner client scenario をすべて CI で実行する。

## シナリオ

standalone FETCH は publisher が live PUBLISH 中に relay からの upstream FETCH request へ応答し、Group 0–1 の Object を返す。`track_probe fetch` は group と object の範囲を指定できる。relay は要求された object offset を保持し、client は要求範囲の外にある Object を `InvalidFetchRange` として拒否する。CI は group 0 object 1 から group 1 object 1 までを取得し、2 Objects を確認する。

late join は publisher が group 0 を公開した後に upstream FETCH を待機し、group 0 を返してから group 1 を live subscription へ送る。relay が保持済みの group 0 を自身で返す場合は upstream FETCH を送らないため、publisher は FETCH の到着か 10 秒の cache window の経過で group 1 へ進む。subscriber は同じ session で FETCH の group 0 と live の group 1 を確認する。publisher 側の待機は `--wait-for-fetch --fetch-after-groups=1` で有効にする。CI は publisher が group 0 を送れるよう、先に同じ track へ warm-up subscriber を接続する。

Namespace withdrawal は既定で実行する。publisher が namespace を撤回した後も独立 track の subscription と Object 配送が継続することを確認する。publisher は relay からの `SUBSCRIBE` と `REQUEST_UPDATE` を受理してから Object stream を開く。session は Forward State が 0 の間、Object stream の開設を拒否する。

joining FETCH の状態遷移と Object 範囲は `src/session/client_joining_fetch.mbt` と Sans-I/O tests で確認する。

Playwright Chromium と WebTransport 実装も Nix flake で固定する。workflow は実行時にバージョンをログへ出す。一時 ECDSA P-256 証明書の正しい SHA-256 pin で接続し、誤った pin の拒否を確認する。WebTransport の URL path は `/` とする。Origin allowlist は relay の設定対象ではなく、この repo では扱わない。

仕様は [Draft 18](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.txt) に固定する。vi64 は QUIC の 62-bit varint と異なり、1〜9 bytes で UInt64 全域を表現する。SETUP の type 0x2f00 は制御ストリームの type も兼ねるので、別のストリームヘッダーを付けない。制御ストリームは SETUP の後も開いたままにする。

## moq-interop-runner クライアント

`track_probe` は [moq-interop-runner](https://github.com/englishm/moq-interop-runner) の test client interface を兼ねる。`--list` で対応する identifier を出力し、各 test case の結果を TAP version 14 と YAML diagnostics で書き出す。diagnostics には duration_ms、session role ごとの negotiated version と transport、QUIC Initial Destination Connection ID、失敗理由を含める。Initial Destination Connection ID は MsQuic の `QUIC_PARAM_CONN_ORIG_DEST_CID` から取得する。

対応する scenario は `setup-only`、`announce-only`、`publish-namespace-done`、`subscribe-namespace-lifecycle`、`publish-namespace-multi-publisher-retention`、`subscribe-error`、`rendezvous-timeout`、`announce-subscribe`、`subscribe-before-announce`、`data-subgroup-basic` の10個である。`data-subgroup-basic` は publisher が routing prefix だけを PUBLISH_NAMESPACE し、subscriber が 16 field の MoQT Test tuple を namespace に持つ SUBSCRIBE を送り、publisher が tuple から導いた 15 objects を subgroup stream で返す。media fixture の生成と decode は client の `--list` に含めない。

`scripts/check-interop.sh` は固定 relay に対して、この10個をすべて実行する。

## ローカルで再現する

~~~sh
nix develop -c moon test
nix develop -c moon check --target js --deny-warn src/js_adapter
nix develop -c moon check --target wasm-gc --deny-warn src/protocol src/session
nix develop -c moon test --target wasm-gc src/protocol src/session
nix develop -c moon test --target js src/protocol src/session src/js_adapter
nix develop -c bash scripts/check-interop.sh
nix develop -c npm ci
nix develop -c bash scripts/check-interop-dual.sh
nix develop -c bash scripts/check-native-leaks.sh
~~~

interop script は Docker daemon と `docker` CLI を必要とする。relay image は初回に pull する。`MOQ_INTEROP_PORT` で host の UDP port を変更でき、`MOQ_INTEROP_RELAY_IMAGE` で image を差し替えられる。実行中の relay がある場合は終了してから実行する。Native QUIC の証明書検証失敗を確認したあと、自己署名証明書の検証を loopback probe に限って無効にする。

手動確認では2つの terminal を使う。まず relay を起動し、次に publisher と subscriber を起動する。両方に同じ relay、path、namespace、track を指定する。

~~~sh
nix develop -c scripts/serve-interop-relay.sh
~~~

~~~sh
nix develop -c moon run --release --target native src/cmd/track_probe -- \
  publish 127.0.0.1 4443 /interop interop multi-object 2 2 --insecure
~~~

~~~sh
nix develop -c moon run --release --target native src/cmd/track_probe -- \
  subscribe 127.0.0.1 4443 /interop interop multi-object 2 2 --insecure
~~~

`check-interop.sh` は一時証明書を生成し、relay を `MOQ_INTEROP_PORT`（既定値 `4443`）の UDP port に loopback で公開する。自己署名証明書が既定の Native 証明書検証で拒否されることを確認した後、loopback probe に限って `--insecure` を使う。終了時に publisher、relay container、一時ファイルを削除する。publisher は Forward State が 1 になるまで Object stream を開かないため、subscriber を先に起動するか、`--wait-for-start` で開始を待つ。

### ネイティブ client のメモリリーク検査

`scripts/check-native-leaks.sh` は固定 relay に対して probe を Valgrind で実行し、definite または possible のリークを検出すると失敗する。Valgrind は Linux でだけ動作する。`MOQ_LEAK_CHECK_PORT` で host の UDP port を変更できる。

### Browser WebTransport 検証

`nix develop -c bash scripts/check-interop-dual.sh` は同じ relay process に raw QUIC と WebTransport の接続を同時に開き、両方で Draft 18 SETUP を確認する。さらに一時 ECDSA P-256 証明書の正しい SHA-256 pin で接続し、誤った pin の拒否と Native/Browser 双方向 publish/subscribe を確認する。各接続と E2E に期限を設定し、失敗時は relay / peer log を出力して子プロセスを終了する。Native probe は loopback の一時自己署名証明書に限り `--insecure` を使う。

Browser WebTransport の connect timeout、遅延した open/write、方向ごとの FIN、送信失敗、close 後に保留 I/O が解放されることは `src/js_adapter/webtransport_test.mbt` の fake-transport tests で決定的に確認する。`check-interop-dual.sh` は Datagram の単独 probe と、Datagram / Subgroup の混在配送を別々に確認する。Sans-I/O tests は peer-side decode と session routing も確認する。

ブラウザだけを手動確認する場合は `nix develop -c scripts/serve-interop-relay.sh` を実行する。表示される URL と証明書 hash を使い、`WebTransport` の `protocols` に `moqt-18` を指定する。

自己署名証明書を使った試験では、ブラウザの WebTransport `serverCertificateHashes` に表示された SHA-256 hash を `Uint8Array` として渡す。これはローカル試験用の明示的な証明書 pin であり、通常の信頼設定や Origin 検証を無効にしない。

ブラウザページでは表示された hash を次のように `Uint8Array` にし、接続を開始する。

~~~js
const hash = Uint8Array.from(atob("BASE64_HASH_FROM_SCRIPT"), c => c.charCodeAt(0))
const transport = new WebTransport("https://127.0.0.1:4443/", {
  protocols: ["moqt-18"],
  serverCertificateHashes: [{ algorithm: "sha-256", value: hash }],
})
await transport.ready
~~~

Origin allowlist はこの repo の対象外とする。証明書 pin の拒否は自動確認する。接続成功はリスナー起動や TLS 成立だけでは判断せず、WebTransport `ready` の後に MoQ Draft 18 SETUP を受信することまで確認する。

CLI の末尾には Group 数と各 Group の Object 数を指定する。probe は `group-<group id>-object-<object id>` 形式の payload を生成し、subscriber 側で各 Group の Object ID 順と payload を照合する。`--object-bytes=BYTES` を指定すると、同じ指定長の決定的な payload を incremental writer で送り、subscriber が長さと内容を照合する。SETUP はどちらのピアからも先に送れるため、probe は相手 SETUP の受信と、自分の SETUP の QUIC ACK の両方を待つ。続く PUBLISH_OK と SUBSCRIBE_OK も確認し、control request が relay を通ることを検証する。publisher は全 Subgroup stream の ACK 後に PUBLISH_DONE を送り、subscriber は各 Object stream の FIN、Object 数、Group 終了、PUBLISH_DONE の stream 数がそろった時点で成功する。

有限 batch の publisher は `PUBLISH_DONE(TRACK_ENDED)` で終了する。subscriber probe は購読 request stream を開いたまま応答と Object を受信し、`PUBLISH_DONE` 後に接続を閉じる。購読更新を検証する lifecycle probe も更新方向を開いたままにする。

publisher は PUBLISH_OK 後に 1 秒待ってから有限 batch を送る。この間に別プロセスの subscriber が relay に接続し、PUBLISH_DONE を含む完了通知を受け取れる。

## 接続先と参照実装

~~~sh
nix develop -c moon run --release --target native src/cmd/setup_probe -- \
  relay.example.com 443 /moq
~~~

引数はホスト、ポート、MoQ URI の path と query。クライアントは ALPN moqt-18 だけを提示し、QUIC DATAGRAM の交渉を確認してから SETUP を送る。Native QUIC 用の PATH、AUTHORITY、MOQT_IMPLEMENTATION を含める。接続から相手の SETUP 受信までの期限は 10 秒。相手の SETUP 受信と自分の SETUP の ACK を待ってから接続を終了する。証明書検証は既定で有効。

## API とリソースの範囲

ClientHandshake は I/O、時計、スレッドを持たない。バージョンは生成時に明示する。受信 SETUP の保持は最大 65,546 bytes に制限し、完了時に保持領域を解放する。エラー後は呼び出し元が対応する close code で接続を終了し、ハンドシェイクを破棄する。

MsQuic の callback は C のメモリだけを操作し、受信データをキューにコピーする。MoonBit の割り当ては呼び出し元がイベントを取り出す際に行う。stream 送信 buffer は `SEND_COMPLETE` まで保持し、そのイベントは QUIC ACK / cancel を示す。Datagram buffer は最終送信状態まで保持し、`DatagramSendStateChanged` は送信、喪失、ack、cancel を区別する。acknowledged は QUIC 層の確認であり、アプリケーションの配送確認ではない。

受信イベントは 1 MiB / 256 件を目安に保持する。この上限に達すると adapter は該当 stream の受信 callback を停止し、アプリケーションが event を取り出して半分以下になった時点で再開する。接続全体は停止しない。単一の receive callback が上限を超える分は、その callback の範囲で一時的に保持する。stream 送信は 1 MiB / 256 件、Datagram 送信は 1 MiB / 64 件まで保持する。ストリーム上限は同時 64 本。上限超過を暗黙のデータ破棄として扱わない。

track_probe は一度に一つの track を publish または subscribe し、各 Group に一つの Subgroup stream を開く。session API は複数の track request と複数の Object stream を管理する。transport.Client の操作は単一の呼び出し元から直列に行い、終了時には close() を呼ぶ。自動 finalizer はない。Native executable は -lmsquic -lpthread を link する。
