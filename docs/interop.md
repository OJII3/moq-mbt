# Draft 18 相互接続

## CI で確認する範囲

CI は Native moq-rs interop、Playwright で制御するブラウザ WebTransport と raw QUIC の moq-rs
SETUP、JavaScript / WebAssembly の check と test を実行する。Browser E2E probe は
Native publisher → Browser subscriber と Browser publisher → Native subscriber の
両方向で Objects、Properties、Group 終端、`PUBLISH_DONE` を確認する。
moq5 は検証 relay として採用しない。Origin 対応と Origin 拒否検証は後続の
[#104](https://github.com/OJII3/moq-mbt/issues/104) で扱うため、この
interop check には含めない。

Native QUIC の interop script は複数 Group/Object の publish/subscribe と `PUBLISH_DONE` を検証する。現在 pin している moq-rs は REQUEST_UPDATE、同一 track の Datagram と Subgroup stream の混在、Standalone FETCH を完了できず、Namespace withdrawal 後に session を閉じる。そのためこれらは relay interop 成功として扱わず、protocol state は Sans-I/O tests で確認する。

moq-rs は1つの QUIC endpoint で raw QUIC と WebTransport を受け付ける。Origin allowlist は CLI に未対応のため、別 issue [#104](https://github.com/OJII3/moq-mbt/issues/104) で扱う。証明書 pin の不一致は dual transport probe で確認する。

仕様は [Draft 18](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.txt) に固定する。vi64 は QUIC の 62-bit varint と異なり、1〜9 bytes で UInt64 全域を表現する。SETUP の type 0x2f00 は制御ストリームの type も兼ねるので、別のストリームヘッダーを付けない。制御ストリームは SETUP の後も開いたままにする。

## ローカルで再現する

~~~sh
moon test
moon check --target js --deny-warn src/js_adapter
moon check --target wasm-gc --deny-warn src/protocol src/session
moon test --target wasm-gc src/protocol src/session
moon test --target js src/protocol src/session src/js_adapter
bash scripts/check-interop.sh
python3 -m venv .venv
.venv/bin/python -m pip install playwright
.venv/bin/python -m playwright install --with-deps chromium
PYTHON=.venv/bin/python bash scripts/check-moq-rs-dual.sh
~~~

Native interop と dual transport のスクリプトはいずれも `moq-rs` を 127.0.0.1:4443 で起動する。実行中の relay がある場合は終了してから実行する。Native QUIC の証明書検証失敗を確認したあと、自己署名証明書の検証を loopback probe に限って無効にする。

手動確認では2つの terminal を使う。まず publisher を起動し、次に別の terminal から subscriber を起動する。両方に同じ relay、path、namespace、track を指定する。

~~~sh
moon run --release --target native src/cmd/track_probe -- \
  publish 127.0.0.1 4443 /moq interop multi-object 2 2 --insecure
~~~

~~~sh
moon run --release --target native src/cmd/track_probe -- \
  subscribe 127.0.0.1 4443 /moq interop multi-object 2 2 --insecure
~~~

`check-interop.sh` は一時証明書を生成し、moq-rs relay を `127.0.0.1:4443` で起動して Native QUIC の Draft 18 interop を確認する。自己署名証明書が既定の Native 証明書検証で拒否されることを確認した後、loopback probe に限って `--insecure` を使う。終了時に publisher、relay、一時ファイルを削除する。

### Browser WebTransport 検証

`PYTHON=.venv/bin/python bash scripts/check-moq-rs-dual.sh` は同じ moq-rs relay process に raw QUIC と WebTransport の接続を同時に開き、両方で Draft 18 SETUP を確認する。さらに一時 ECDSA P-256 証明書の正しい SHA-256 pin で接続し、誤った pin の拒否と Native/Browser 双方向 publish/subscribe を確認する。Native probe は loopback の一時自己署名証明書に限り `--insecure` を使う。

ブラウザだけを手動確認する場合は `scripts/serve-moq-rs.sh` を実行する。表示される URL と証明書 hash を使い、`WebTransport` の `protocols` に `moqt-18` を指定する。

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

moq-rs の現在の CLI には WebTransport Origin allowlist がない。Origin 拒否は [#104](https://github.com/OJII3/moq-mbt/issues/104) の対象とする。証明書 pin の拒否は自動確認する。接続成功はリスナー起動や TLS 成立だけでは判断せず、WebTransport `ready` の後に MoQ Draft 18 SETUP を受信することまで確認する。

CLI の末尾には Group 数と各 Group の Object 数を指定する。probe は `group-<group id>-object-<object id>` 形式の payload を生成し、subscriber 側で各 Group の Object ID 順と payload を照合する。SETUP はどちらのピアからも先に送れるため、probe は相手 SETUP の受信と、自分の SETUP の QUIC ACK の両方を待つ。続く PUBLISH_OK と SUBSCRIBE_OK も確認し、control request が relay を通ることを検証する。publisher は全 Subgroup stream の ACK 後に PUBLISH_DONE を送り、subscriber は Object 数、Group 終了、PUBLISH_DONE の stream 数がそろった時点で成功する。

有限 batch の publisher は `PUBLISH_DONE(TRACK_ENDED)` で終了する。通常の subscriber probe は更新を送らないため、`finish_subscription_updates` で SUBSCRIBE の送信方向を先に FIN する。応答と Object の受信は継続し、PUBLISH_DONE の受信条件は緩和しない。購読更新を検証する lifecycle probe は更新方向を開いたままにする。

publisher は PUBLISH_OK 後に 1 秒待ってから有限 batch を送る。この間に別プロセスの subscriber が relay に接続し、PUBLISH_DONE を含む完了通知を受け取れる。

`lifecycle`、Namespace、FETCH、Datagram probe は個別に実装されているが、現在 pin している moq-rs で成功する相互運用シナリオとしては扱わない。REQUEST_UPDATE は relay に未実装で、Namespace withdrawal 後は relay が session を終了し、FETCH は deadline までに終わらず、同一 track の Datagram と Subgroup stream 混在時は relay が `multiple stream modes` を返す。更新失敗や relay 発 GOAWAY の再現には時間待ちを使わず、Sans-I/O の session tests を使う。

## 接続先と参照実装

~~~sh
moon run --release --target native src/cmd/setup_probe -- \
  relay.example.com 443 /moq
~~~

引数はホスト、ポート、MoQ URI の path と query。クライアントは ALPN moqt-18 だけを提示し、QUIC DATAGRAM の交渉を確認してから SETUP を送る。Native QUIC 用の PATH、AUTHORITY、MOQT_IMPLEMENTATION を含める。接続から相手の SETUP 受信までの期限は 10 秒。相手の SETUP 受信と自分の SETUP の ACK を待ってから接続を終了する。証明書検証は既定で有効。

Relay は [cloudflare/moq-rs の固定コミット](https://github.com/cloudflare/moq-rs/tree/8779758892d921bcfccbb6e4d9319c149db28c91) を Cargo でビルドする。Native QUIC と WebTransport は同じ endpoint の ALPN で選択される。

## API とリソースの範囲

ClientHandshake は I/O、時計、スレッドを持たない。バージョンは生成時に明示する。受信 SETUP の保持は最大 65,546 bytes に制限し、完了時に保持領域を解放する。エラー後は呼び出し元が対応する close code で接続を終了し、ハンドシェイクを破棄する。

MsQuic の callback は C のメモリだけを操作し、受信データをキューにコピーする。MoonBit の割り当ては呼び出し元がイベントを取り出す際に行う。stream 送信 buffer は `SEND_COMPLETE` まで保持し、そのイベントは QUIC ACK / cancel を示す。Datagram buffer は最終送信状態まで保持し、`DatagramSendStateChanged` は送信、喪失、ack、cancel を区別する。acknowledged は QUIC 層の確認であり、アプリケーションの配送確認ではない。受信イベントは 1 MiB / 256 件、stream 送信は 1 MiB / 256 件、Datagram 送信は 1 MiB / 64 件まで保持する。ストリーム上限は同時 16 本。上限超過を暗黙のデータ破棄として扱わない。

track_probe は一度に一つの track を publish または subscribe し、各 Group に一つの Subgroup stream を開く。session API は複数の track request と複数の Object stream を管理する。transport.Client の操作は単一の呼び出し元から直列に行い、終了時には close() を呼ぶ。自動 finalizer はない。Native executable は -lmsquic -lpthread を link する。
