package electric4k

import kotlinx.datetime.*
import kotlin.math.pow
import kotlin.random.Random

/** maxAttempts includes the initial request. Cancellation and protocol errors are never retried. */
data class RetryPolicy(val maxAttempts: Int = Int.MAX_VALUE, val initialDelayMillis: Long = 1000, val maxDelayMillis: Long = 32_000, val multiplier: Double = 2.0, val onFailedAttempt: (() -> Unit)? = null, val debug: Boolean = false) {
    init { require(maxAttempts > 0); require(multiplier.isFinite() && multiplier >= 1.0); require(initialDelayMillis >= 0 && maxDelayMillis >= initialDelayMillis) }
    internal fun delayMillis(attempt: Int, retryAfter: Long?): Long {
        val cap = (initialDelayMillis * multiplier.pow(attempt.coerceAtMost(1024))).coerceAtMost(maxDelayMillis.toDouble()).toLong()
        val wait = maxOf(retryAfter ?: 0, if (cap == 0L) 0 else Random.nextLong(0, cap))
        if (debug) println("Electric4k retry ${attempt + 1} after ${wait}ms")
        return wait
    }
}
internal fun retryAfterMillis(value: String?, now: Instant = Clock.System.now()): Long? {
    if (value == null) return null
    value.trim().toDoubleOrNull()?.takeIf { it.isFinite() }?.let { return (it.coerceAtLeast(0.0) * 1000).toLong() }
    return runCatching {
        val parts = value.substringAfter(", ").split(' ')
        require(parts.size == 5 && parts[4] == "GMT")
        val month = listOf("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec").indexOf(parts[1]) + 1
        val time = parts[3].split(':').map(String::toInt)
        val date = LocalDateTime(parts[2].toInt(), month, parts[0].toInt(), time[0], time[1], time[2])
        (date.toInstant(TimeZone.UTC).toEpochMilliseconds() - now.toEpochMilliseconds()).coerceIn(0, 3_600_000)
    }.getOrNull()
}
