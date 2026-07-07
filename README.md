# ActivityPub Relay Proxy (仮想リレープロキシ)

Mastodon などの ActivityPub サーバーのための**仮想リレープロキシ**

---

## 前提条件

- **Go**: 1.25 以上 (ローカルビルド時)
- **Redis**: 7.2 以上
- **Docker / Docker Compose** (推奨)

---

## セットアップ

### 1. 環境変数の設定

`.env.example` をコピーして `.env` を作成し、環境に合わせて編集します。

```bash
cp .env.example .env
```

#### 主要な設定項目

| 環境変数 | 説明 | 設定例 |
| :--- | :--- | :--- |
| `PORT` | プロキシサーバーがリッスンするポート番号。 | `8080` |
| `DOMAIN` | プロキシを公開するドメイン名（SSL/TLS必須）。 | `relay.example.com` |
| `REDIS_URL` | Redis への接続 URL。 | `redis://redis:6379/0` (Compose時) |
| `PRIVATE_KEY` | 仮想アクター用の RSA 秘密鍵を **Base64** エンコードした文字列（改行なし）。 | (生成方法は後述) |
| `ACTOR_USERNAME` | 仮想アクターのユーザー名。 | `relay` |
| `DESTINATION_URL` | 配信先となるあなた自身の Mastodon インスタンス URL。 | `https://mastodon.example.com` |
| `FILTER_KEYWORDS` | フィルタリングするキーワード（カンマ区切り）。 | `mastodon,activitypub` |
| `FILTER_HASHTAGS` | フィルタリングするハッシュタグ（カンマ区切り）。 | `relay,proxy` |
| `DEDUPLICATION_TTL` | 重複排除キャッシュの保持期間。 | `24h` |

### 2. 秘密鍵の生成と設定

仮想アクターが署名を付与するために、RSA 秘密鍵が必要です。

1. **RSA 秘密鍵の生成**:
   ```bash
   openssl genrsa -out private.pem 2048
   ```

2. **Base64 エンコード**:
   改行コードを含めずに1行の文字列としてエンコードし、`.env` の `PRIVATE_KEY` に設定します。
   - **Linux**:
     ```bash
     cat private.pem | base64 -w 0
     ```
   - **macOS**:
     ```bash
     cat private.pem | base64 | tr -d '\n'
     ```

---

## 起動方法

### A. Docker Compose を使用する

最も簡単な起動方法です。Redis コンテナと Go アプリコンテナが一緒に立ち上がります。

```bash
docker compose up -d
```

## ライセンス

[MIT License](LICENSE)