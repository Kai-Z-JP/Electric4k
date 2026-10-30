# Electric4k

[Electric](https://github.com/electric-sql/electric) の非公式 Kotlin Multiplatform 同期クライアント。

![Platforms](https://img.shields.io/badge/platforms-JVM%20%7C%20Android%20%7C%20JS%20%7C%20Wasm%20%7C%20iOS-7F52FF)
[![Upstream](https://img.shields.io/static/v1?label=upstream&message=%40electric-sql%2Fclient%201.5.28&color=blue)](docs/upstream.json)

## 使い方

```kotlin
val client = ShapeClient("https://your-api.example/v1/shape")
val stream = ShapeStream(client, ShapeRequest("todos"), scope)
val shape = Shape(stream)

val rows = shape.awaitRows()
val unsubscribe = shape.subscribe { change -> render(change.rows) }

// 終了時
unsubscribe()
shape.close()
stream.close()
client.close()
```

`scope` はアプリの CoroutineScope、`render` は表示処理です。`Shape` は行を保持し、`ShapeStream.messages()` は変更メッセージを Flow で返します。

## 開発

JDK 21、Android SDK 35、Build Tools 35.0.0 を使用します。
実行環境は JVM 17 以上、Android API 23 以上です。

```sh
./gradlew jvmTest
./gradlew jsNodeTest
./gradlew jsBrowserTest wasmJsBrowserTest  # Chrome / Chromium が必要
./gradlew testDebugUnitTest assembleRelease
./scripts/integration.sh          # Docker が必要

# macOS + Xcode
./gradlew iosSimulatorArm64Test linkReleaseFrameworkIosArm64
```

既存の Node.js を使う場合は `-Pelectric4k.systemNode=true` を指定します。

## ライセンス

[Apache-2.0](LICENSE) · [出典](NOTICE)
