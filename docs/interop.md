# Draft 18 multi-object / multi-group 疎通

Native QUIC 上で Draft 18 の SETUP 交換、単一トラックの publish/subscribe、複数 Group にまたがる複数 Object の配送を確認する。probe は各 Group を信頼性のある Subgroup stream に載せ、Object ID の順序、payload の一致、Group の終了、PUBLISH_DONE の stream 数を検証する。moq5 では同じ connection で Object Datagram も送り、stream と混在した受信を確認する。Datagram smoke は MsQuic が通知した最大長、1 byte 超過、送信 queue 上限、pending send がある状態での close を検証する。protocol は vi64、control message framing、SUBSCRIBE/PUBLISH と Subgroup object を扱う。session は複数 track request と複数 Object stream を管理し、分割受信を組み立てる。

仕様は [Draft 18](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.txt) に固定する。vi64 は QUIC の 62-bit varint と異なり、1〜9 bytes で UInt64 全域を表現する。SETUP の type 0x2f00 は制御ストリームの type も兼ねるので、別のストリームヘッダーを付けない。制御ストリームは SETUP の後も開いたままにする。

## ローカルで再現する

~~~sh
nix develop -c moon test
nix develop -c moon test --target wasm
nix develop -c moon test --target js
nix develop -c bash scripts/check-interop.sh
~~~

疎通スクリプトは 127.0.0.1:4443 を使う。実行中のリレーがある場合は終了してから実行する。証明書検証に失敗する接続を確認したあと、SETUP、track 登録、object 配送を確認する。成功時には stream と Object Datagram の両方が配送される。

~~~text
QUIC connected: ALPN moqt-18, DATAGRAM negotiated
MoQT Draft 18 SETUP complete (0 options)
PUBLISH request accepted
Subscription accepted (track alias 1)
Sent group 0 (2 Objects)
Sent group 1 (2 Objects)
Sent group 2 (2 Objects)
PUBLISH_DONE send completed
Received Object (group 0, object 0, 16 bytes)
Received Object (group 0, object 1, 16 bytes)
Received Object (group 1, object 0, 16 bytes)
Received Object (group 1, object 1, 16 bytes)
Received Object (group 2, object 0, 16 bytes)
Received Object (group 2, object 1, 16 bytes)
Verified 6 Objects across 3 groups; PUBLISH_DONE received
Client and relay SETUP exchange completed
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

publisher は PUBLISH_OK 後に 1 秒待ってから有限 batch を送る。この間に別プロセスの subscriber が relay に接続し、PUBLISH_DONE を含む完了通知を受け取れる。

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
