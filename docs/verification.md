# 形式検証

`src/lifecycle` はリクエストのライフサイクル遷移を純粋関数として持ち、`moon prove` で検証する。`src/session` はこの状態機械を呼び出すシェルとして、Map の所有と transport へのアクション生成を担う。

## 検証している範囲

`RequestLifetime::new` と `RequestLifetime::transition` に契約を置いている。`new` は両方向が開いていて、キャンセルされておらず、エラーコードと終了通知がないことを示す。`transition` の事後条件は次の性質を要求する。

- `Cancel`、`PeerReset`、`PeerStop` の後は `cancelled` が真になる。
- 両方向の closed フラグ、`cancelled`、`end_notified` は真から偽に戻らない。
- `notify_end` は `end_notified` が偽のときだけ真になり、遷移後の状態では `end_notified` が真になる。
- `notify_end` が偽なら `end_notified` は変化しない。
- `abort_send` はローカル送信方向がまだ開いているときだけ真になる。
- `stop_receiving` はピア送信方向がまだ開いているときだけ真になる。
- 一度設定された `cancel_error_code` は別の terminal イベントで上書きされない。

表現不変条件は `cancel_error_code` が設定されているなら `cancelled` が真であることを要求する。`end_notified` は名前空間購読の FIN 終了でも単独で立つため、この不変条件には含めない。

## 検証していない範囲

`with_end_notified`、`with_local_send_closed`、`terminated` は名前空間購読の既存動作を保つための単純なコピーであり、契約を付けていない。`transition` の検証はこれらの関数を経由した状態変更を対象にしない。

`ClientSession`、`SessionDriver`、transport adapter は検証対象外である。`ClientSession` は可変な Map と `Bytes` を多数持ち、現在の `moon prove` が扱いやすい関数型の断片から外れる。transport のイベント順序やスレッド境界も、証明ではなく adapter の契約とテストで扱う。

## 実行方法

devShell に z3 が入っているため、次で実行する。

```sh
nix develop -c moon prove src/lifecycle
```

`moon prove` は solver を PATH から探す。solver がない環境では、Why3 の設定や `Z3PATH` の指定が必要になる。

## 現時点の制約

`moon prove` は現状、可変な構造体フィールドへの代入、`Option` に対する `is None`、契約内のタプル射影を扱えない。そのため、検証する遷移は不変な構造体と名前付きの結果型で書く。整数は数学的整数として扱われるため、`UInt64` のオーバーフローは別の範囲制約で扱う必要がある。

ライブネス、公平性、RFC への適合性はこの検証の対象ではない。これらは interop テストとモデルベースのトレーステストで補う。

## 次の候補

- `PartialObject` の長さ会計。`remaining` が宣言長から書き込み量を引いた値に一致し、`finish` が残量ゼロのときだけ成功することを示す。
- Request ID と Track Alias の割当。単調増加と未使用値の割当を、Map のモデルを定義して示す。
- `vi64` の encode/decode 往復。`Bytes` と bit 演算が証明の断片に入るか、小さな実験で確認する。
