# Draft 18 publish/subscribe 疎通

Native QUIC 上で Draft 18 の SETUP 交換、単一トラックの publish/subscribe、信頼性のある Subgroup stream による 1 object の配送を確認する。protocol は vi64、control message framing、SUBSCRIBE/PUBLISH と Subgroup object を扱う。session は一つの track と subscriber の状態を持ち、分割受信を組み立てる。

仕様は [Draft 18](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.txt) に固定する。vi64 は QUIC の 62-bit varint と異なり、1〜9 bytes で UInt64 全域を表現する。SETUP の type 0x2f00 は制御ストリームの type も兼ねるので、別のストリームヘッダーを付けない。制御ストリームは SETUP の後も開いたままにする。

## ローカルで再現する

~~~sh
nix develop -c moon test
nix develop -c moon test --target wasm
nix develop -c moon test --target js
nix develop -c bash scripts/check-interop.sh
~~~

疎通スクリプトは 127.0.0.1:4443 を使う。実行中のリレーがある場合は終了してから実行する。証明書検証に失敗する接続を確認したあと、SETUP、track 登録、object 配送を確認する。成功時には次のような出力が得られる。

~~~text
QUIC connected: ALPN moqt-18, DATAGRAM negotiated
MoQT Draft 18 SETUP complete (0 options)
Track publication accepted
Subscription accepted (track alias 1)
Sent one Object
Received Object (group 0, object 0, 10 bytes)
Subscription finished
Client and relay SETUP exchange completed
~~~

手動確認では2つの terminal を使う。まず publisher を起動し、次に別の terminal から subscriber を起動する。両方に同じ relay、path、namespace、track を指定する。

~~~sh
nix develop -c moon run --release --target native src/cmd/track_probe -- \
  publish 127.0.0.1 4443 /moq interop one-object 'one object' --insecure
~~~

~~~sh
nix develop -c moon run --release --target native src/cmd/track_probe -- \
  subscribe 127.0.0.1 4443 /moq interop one-object --insecure
~~~

疎通スクリプトは一時ディレクトリに証明書と設定を生成し、moq5 を versions: [18]、認証 allow_all で起動する。自己署名証明書が既定の証明書検証で拒否されることを確認した後、ループバック接続に限って --insecure を使う。終了時に publisher、relay、一時ファイルを削除する。

SETUP はどちらのピアからも先に送れるため、probe は相手 SETUP の受信と、自分の SETUP の QUIC ACK の両方を待つ。続く PUBLISH_OK と SUBSCRIBE_OK も確認し、control request が relay を通ることを検証する。

## 接続先と参照実装

~~~sh
nix develop -c moon run --release --target native src/cmd/setup_probe -- \
  relay.example.com 443 /moq
~~~

引数はホスト、ポート、MoQ URI の path と query。クライアントは ALPN moqt-18 だけを提示し、QUIC DATAGRAM の交渉を確認してから SETUP を送る。Native QUIC 用の PATH、AUTHORITY、MOQT_IMPLEMENTATION を含める。接続から相手の SETUP 受信までの期限は 10 秒。相手の SETUP 受信と自分の SETUP の ACK を待ってから接続を終了する。証明書検証は既定で有効。

リレーは [openmoq/moq5 の固定コミット](https://github.com/openmoq/moq5/tree/56dd3421ee95a4227c07c2091718d522fc8a5b25) を nix/moq5-relay.nix でビルドし、既存の libmsquic を使う。Nixpkgs の moq は Go のモック生成ツールなので使用しない。moq5 の実行方法と設定は[同コミットの relay README](https://github.com/openmoq/moq5/blob/56dd3421ee95a4227c07c2091718d522fc8a5b25/tools/moq5-relay/README.md)を参照する。

## API とリソースの範囲

ClientHandshake は I/O、時計、スレッドを持たない。バージョンは生成時に明示する。受信 SETUP の保持は最大 65,546 bytes に制限し、完了時に保持領域を解放する。エラー後は呼び出し元が対応する close code で接続を終了し、ハンドシェイクを破棄する。

MsQuic の callback は C のメモリだけを操作し、受信データをキューにコピーする。MoonBit の割り当ては呼び出し元がイベントを取り出す際に行う。送信バイト列もコピーし、SEND_COMPLETE まで保持する。MsQuic の送信バッファリングを無効にし、成功の SEND_COMPLETE はピアの ACK 後に通知する。送受信それぞれの保持上限は 1 MiB、イベントと未完了送信は各 256 件、ストリームは同時 16 本。上限超過を暗黙のデータ破棄として扱わない。

現時点の session API は単一トラックと一つの subscriber を扱う。object は信頼性のある Subgroup stream 上で一つだけ送受信する。transport.Client の操作は単一の呼び出し元から直列に行い、終了時には close() を呼ぶ。自動 finalizer はない。Native executable は -lmsquic -lpthread を link する。
