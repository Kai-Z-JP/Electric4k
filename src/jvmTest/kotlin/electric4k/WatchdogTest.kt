package electric4k

import io.ktor.client.*
import io.ktor.client.engine.mock.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import kotlin.test.*

/** Real clock: an independent timeout must cancel a request which never completes. */
class WatchdogTest {
    @Test fun hungLiveRequestIsCancelledAndRefreshed() = runBlocking {
        var count = 0
        val cancelled = CompletableDeferred<Unit>()
        val http = HttpClient(MockEngine { request ->
            when (++count) {
                1 -> respond("[{\"headers\":{\"control\":\"up-to-date\"}}]", headers = responseHeaders())
                2 -> try { awaitCancellation() } finally { cancelled.complete(Unit) }
                3 -> {
                    assertNull(request.url.parameters["live"])
                    respond("[{\"headers\":{\"control\":\"up-to-date\"}}]", headers = responseHeaders("20_0"))
                }
                else -> awaitCancellation()
            }
        })
        val stream = ShapeStream(ShapeClient("https://example.test/shape", http), ShapeRequest("todos"), this,
            ShapeStreamOptions(liveRequestTimeoutMillis = 100, detectSystemWake = false), ProtocolCache())
        try {
            stream.subscribe({})
            withTimeout(10_000) { cancelled.await(); stream.status.first { it.offset == "20_0" } }; Unit
        } finally { stream.close(); http.close() }
    }
}
