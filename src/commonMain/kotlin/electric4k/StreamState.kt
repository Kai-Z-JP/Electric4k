package electric4k

/** Port of the pinned upstream SPEC.md state transitions. Transitions never mutate their input. */
internal enum class StreamKind { Initial, Syncing, Live, Replaying, StaleRetry, Paused, Error }
internal data class StreamFields(
    val handle: String? = null,
    val offset: String = "-1",
    val schema: ShapeSchema? = null,
    val liveCacheBuster: String = "",
    val lastSyncedAt: Long? = null,
)
internal data class ResponseMetadata(
    val status: Int, val handle: String? = null, val offset: String? = null,
    val cursor: String? = null, val schema: ShapeSchema? = null, val expiredHandle: String? = null,
    val now: Long, val maxStaleRetries: Int = 3, val cacheBuster: () -> String,
)
internal data class ResponseTransition(val state: StreamState, val action: String = "accepted", val exceededMaxRetries: Boolean = false)
internal data class MessageTransition(val state: StreamState, val suppressUpToDate: Boolean = false, val becameUpToDate: Boolean = false)
internal data class SseTransition(val state: StreamState, val fellBack: Boolean = false, val wasShort: Boolean = false)

internal class StreamState private constructor(
    val kind: StreamKind,
    private val ownFields: StreamFields = StreamFields(),
    val previous: StreamState? = null,
    private val staleBuster: String? = null,
    private val staleCount: Int = 0,
    private val fallback: Boolean = false,
    private val shortConnections: Int = 0,
    private val replay: String? = null,
    val error: Throwable? = null,
) {
    val fields: StreamFields get() = previous?.fields ?: ownFields
    val isUpToDate: Boolean get() = previous?.isUpToDate ?: (kind == StreamKind.Live)
    val staleCacheBuster: String? get() = previous?.staleCacheBuster ?: staleBuster
    val staleCacheRetryCount: Int get() = previous?.staleCacheRetryCount ?: staleCount
    val sseFallback: Boolean get() = previous?.sseFallback ?: fallback
    val consecutiveShortSseConnections: Int get() = previous?.consecutiveShortSseConnections ?: shortConnections
    val replayCursor: String? get() = previous?.replayCursor ?: replay

    fun pause(): StreamState = if (kind == StreamKind.Paused) this else StreamState(StreamKind.Paused, previous = this)
    fun resume(): StreamState = if (kind == StreamKind.Paused) previous!! else this
    fun toError(error: Throwable): StreamState = StreamState(StreamKind.Error, previous = if (kind == StreamKind.Error) previous else this, error = error)
    fun retry(): StreamState = if (kind == StreamKind.Error) previous!! else this
    fun reset(handle: String? = null): StreamState = initial(StreamFields(handle = handle, lastSyncedAt = fields.lastSyncedAt))
    fun withHandle(handle: String): StreamState = if (previous != null) {
        StreamState(kind, previous = previous.withHandle(handle), error = error)
    } else StreamState(kind, fields.copy(handle = handle), staleBuster = staleBuster, staleCount = staleCount, fallback = fallback, shortConnections = shortConnections, replay = replay)
    fun canReplay(): Boolean = kind == StreamKind.Initial || kind == StreamKind.Syncing
    fun enterReplay(cursor: String): StreamState = if (canReplay() || kind == StreamKind.StaleRetry) StreamState(StreamKind.Replaying, fields, replay = cursor) else this
    fun shouldUseSse(enabled: Boolean, refreshing: Boolean, resuming: Boolean): Boolean = kind == StreamKind.Live && enabled && !refreshing && !resuming && !fallback

    fun parameters(snapshot: Boolean = false, canLongPoll: Boolean = true): Map<String, String> {
        previous?.let { return it.parameters(snapshot, canLongPoll) }
        return buildMap {
            put("offset", fields.offset)
            fields.handle?.let { put("handle", it) }
            if (kind == StreamKind.StaleRetry) put("cache-buster", staleBuster!!)
            if (kind == StreamKind.Live && !snapshot) {
                put("cursor", fields.liveCacheBuster)
                if (canLongPoll) put("live", "true")
            }
        }
    }
    fun response(input: ResponseMetadata): ResponseTransition {
        if (kind == StreamKind.Error) return ResponseTransition(this, "ignored")
        if (kind == StreamKind.Paused) {
            val transition = previous!!.response(input)
            return if (transition.action == "ignored") ResponseTransition(this, "ignored") else transition.copy(state = transition.state.pause())
        }
        if (input.handle != null && input.handle == input.expiredHandle) {
            val count = staleCacheRetryCount + 1
            return ResponseTransition(StreamState(StreamKind.StaleRetry, fields, staleBuster = input.cacheBuster(), staleCount = count), "stale-retry", count > input.maxStaleRetries)
        }
        val next = fields.copy(
            handle = input.handle?.takeIf { it.isNotEmpty() && it != input.expiredHandle } ?: fields.handle,
            offset = input.offset ?: fields.offset,
            schema = fields.schema ?: input.schema,
            liveCacheBuster = input.cursor ?: fields.liveCacheBuster,
            lastSyncedAt = if (input.status == 204) input.now else fields.lastSyncedAt,
        )
        return ResponseTransition(when (kind) {
            StreamKind.Live -> StreamState(StreamKind.Live, next, fallback = fallback, shortConnections = shortConnections)
            StreamKind.Replaying -> StreamState(StreamKind.Replaying, next, replay = replay)
            else -> if (input.status == 204) StreamState(StreamKind.Live, next, fallback = true) else StreamState(StreamKind.Syncing, next)
        })
    }
    fun messages(hasMessages: Boolean, upToDate: Boolean, sse: Boolean, offset: String?, now: Long, cursor: String): MessageTransition {
        if (previous != null || !hasMessages || !upToDate) return MessageTransition(this)
        val next = fields.copy(offset = if (sse) offset ?: fields.offset else fields.offset, lastSyncedAt = now)
        val result = if (kind == StreamKind.Live) StreamState(StreamKind.Live, next, fallback = fallback, shortConnections = shortConnections) else StreamState(StreamKind.Live, next)
        return MessageTransition(result, kind == StreamKind.Replaying && !sse && replay == cursor, true)
    }
    fun sseClosed(durationMillis: Long, aborted: Boolean, minimumMillis: Long = 1000, maximumShortConnections: Int = 3): SseTransition {
        if (kind != StreamKind.Live) return SseTransition(this)
        val short = durationMillis < minimumMillis && !aborted
        val count = if (short) shortConnections + 1 else if (durationMillis >= minimumMillis) 0 else shortConnections
        val fellBack = short && count >= maximumShortConnections
        return SseTransition(StreamState(StreamKind.Live, fields, fallback = fallback || fellBack, shortConnections = count), fellBack, short)
    }
    companion object {
        fun initial(fields: StreamFields = StreamFields()): StreamState = StreamState(StreamKind.Initial, fields)
    }
}
