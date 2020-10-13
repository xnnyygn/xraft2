package `in`.xnnyygn.xraft2.election

import `in`.xnnyygn.xraft2.cell.CellContext
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

internal class ElectionTimeout(private val delay: Long) {
    private var scheduledAt: Long = -1L
    private var scheduledFuture: ScheduledFuture<*>? = null

    val isValid: Boolean
        get() = (System.currentTimeMillis() - scheduledAt) > delay

    fun clear() {
        scheduledFuture = null
    }

    fun cancel(context: CellContext) {
        val sf = scheduledFuture
        if (sf == null) {
            context.logger.warn("no election timeout to cancel")
        } else {
            sf.cancel(true)
            scheduledFuture = null
        }
    }

    // remember to call clear() first unless system is starting
    fun schedule(context: CellContext) {
        // scheduledFuture == null
        scheduledAt = System.currentTimeMillis()
        scheduledFuture = context.schedule(delay, TimeUnit.MILLISECONDS, ElectionTimeoutEvent)
    }
}