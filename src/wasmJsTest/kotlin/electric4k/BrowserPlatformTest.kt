package electric4k

import kotlin.js.JsAny
import kotlin.JsFun
import kotlin.test.*
import kotlinx.coroutines.test.runTest

@JsFun("() => document.dispatchEvent(new Event('visibilitychange'))")
private external fun dispatchVisibility()
@JsFun("() => { const original = globalThis.fetch; globalThis.fetch = async () => new Response('[{\"headers\":{\"control\":\"up-to-date\"}}]', {headers: {'content-type':'application/json', 'electric-offset':'10_0', 'electric-handle':'wasm-test', 'electric-schema':'{}'}}); return original; }")
private external fun installFetch(): JsAny
@JsFun("(original) => { globalThis.fetch = original; }")
private external fun restoreFetch(original: JsAny)

class BrowserPlatformTest {
    @Test fun checkpointSurvivesNewStoreAndCanBeCleared() = runTest {
        val prefix = "electric4k-wasm-test:"
        val store = LocalStorageShapeStore(prefix)
        val shape = StoredShape("todos", cursor = ShapeCursor("10_0", "wasm-test"))
        try {
            store.save("checkpoint", shape)
            assertEquals(shape, LocalStorageShapeStore(prefix).load("checkpoint"))
            store.clear("checkpoint")
            assertNull(store.load("checkpoint"))
        } finally { store.clear("checkpoint") }
    }
    @Test fun visibilityListenerCanBeRemoved() {
        val adapter = assertNotNull(platformRuntimeVisibility())
        var calls = 0
        val unsubscribe = adapter.subscribe { visible -> assertEquals(adapter.isVisible(), visible); calls++ }
        try { dispatchVisibility(); assertEquals(1, calls) }
        finally { unsubscribe() }
        dispatchVisibility()
        assertEquals(1, calls)
    }
    @Test fun defaultKtorEngineReadsBrowserFetchResponse() = runTest {
        val original = installFetch()
        val client = ShapeClient("https://example.test/shape")
        try {
            val batch = client.fetch("todos")
            assertEquals(ShapeCursor("10_0", "wasm-test"), batch.cursor)
            assertTrue(batch.upToDate)
        } finally { client.close(); restoreFetch(original) }
    }
}
