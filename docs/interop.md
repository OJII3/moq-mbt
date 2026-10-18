# Draft 18 SETUP 疎通

最初の実装範囲は、Native QUIC 上でのクライアントの SETUP 交換とする。
`protocol` は Draft 18 の vi64、16-bit 長さのメッセージ framing、delta encoded
Setup Options を扱う。`session.ClientHandshake` は分割受信を組み立て、相手の
SETUP と後続の制御メッセージのバイト列を返す。後続メッセージ、リクエスト、
オブジェクトは今後実装する。

仕様は [Draft 18](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.txt)
に固定する。特に vi64 は QUIC の 62-bit varint と異なり、1〜9 bytes で UInt64
全域を表現する。SETUP の type `0x2f00` は制御ストリームの type も兼ねるので、
別のストリームヘッダーを付けない。制御ストリームは SETUP の後も開いたままにする。

## ローカルで再現する

```sh
nix develop -c moon test
nix develop -c moon test --target wasm
nix develop -c moon test --target js
nix develop -c bash scripts/check-setup-interop.sh
```

疎通スクリプトは `127.0.0.1:4443` を使用する。実行中のリレーがある場合は終了してから
実行する。成功時は次の出力が得られる。

```text
QUIC connected: ALPN moqt-18, DATAGRAM negotiated
MoQT Draft 18 SETUP received (0 options)
Client SETUP acknowledged
Relay accepted the client SETUP
```

スクリプトは一時ディレクトリに証明書と設定を生成し、moq5 を `versions: [18]`、
認証 `allow_all` で起動する。自己署名証明書が既定の証明書検証で拒否されることを
確認した後、ループバックの疎通に限って `--insecure` を使用する。終了時にリレーと
一時ファイルを削除する。

SETUP はどちらのピアからも先に送れるため、相手の SETUP 受信だけではこちらの
SETUP が処理されたかは分からない。moq5 のトレースに SETUP の認可成功が記録される
ことも確認し、双方向の処理を検証する。

## 接続先と参照実装

```sh
nix develop -c moon run --release --target native src/cmd/setup_probe -- \
  relay.example.com 443 /moq
```

引数はホスト、ポート、MoQ URI の path と query。クライアントは ALPN `moqt-18`
だけを提示し、QUIC DATAGRAM の交渉を確認してから SETUP を送る。Native QUIC 用の
PATH、AUTHORITY、MOQT_IMPLEMENTATION を含める。接続から相手の SETUP 受信までの
期限は 10 秒。相手の SETUP 受信と自分の SETUP の ACK を待ってから接続を終了する。
証明書検証は既定で有効。

リレーは [openmoq/moq5 の固定コミット](https://github.com/openmoq/moq5/tree/56dd3421ee95a4227c07c2091718d522fc8a5b25)
を `nix/moq5-relay.nix` でビルドし、既存の `libmsquic` を使う。Nixpkgs の `moq`
は Go のモック生成ツールなので使用しない。moq5 の実行方法と設定は
[同コミットの relay README](https://github.com/openmoq/moq5/blob/56dd3421ee95a4227c07c2091718d522fc8a5b25/tools/moq5-relay/README.md)
を参照する。

## API とリソースの範囲

`ClientHandshake` は I/O、時計、スレッドを持たない。バージョンは生成時に明示する。
受信 SETUP の保持は最大 65,546 bytes に制限し、完了時に保持領域を解放する。
エラー後は呼び出し元が対応する close code で接続を終了し、ハンドシェイクを破棄する。

MsQuic の callback は C のメモリだけを操作し、受信データをキューにコピーする。
MoonBit の割り当ては呼び出し元がイベントを取り出す際に行う。送信バイト列もコピーし、
SEND_COMPLETE まで保持する。MsQuic の送信バッファリングを無効にし、成功の
SEND_COMPLETE はピアの ACK 後に通知する。送受信それぞれの保持上限は 1 MiB、イベントと未完了送信は
各 256 件、ストリームは接続中の累計 16 本。上限超過を暗黙のデータ破棄として扱わない。
今後の publish/subscribe 実装では、ストリームの再利用とバックプレッシャーを拡張する。

`transport.Client` の操作は単一の呼び出し元から直列に行い、終了時には `close()` を
呼ぶ。自動 finalizer はない。リンクする Native executable の `moon.pkg` に
`-lmsquic -lpthread` を指定する。例は `src/cmd/setup_probe/moon.pkg` にある。

現在の既定ターゲットは Native とし、`moon info` で Native 専用パッケージも含めた
公開インターフェースを生成する。`protocol` と `session` は Wasm、JavaScript、Native
のテスト対象とする。
