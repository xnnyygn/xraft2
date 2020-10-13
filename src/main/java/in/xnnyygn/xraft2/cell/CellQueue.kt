package `in`.xnnyygn.xraft2.cell

import `in`.xnnyygn.xraft2.getLogger
import java.util.*

/**
 * A queue for cell.
 * Current implementation is a wrapper class of [LinkedList].
 * All methods are synchronized.
 */
class CellQueue {
    companion object {
        val logger = getLogger(CellQueue::class.java)
    }

    private var consuming = false
    private val queue = LinkedList<Event>()
    private var blocker: CellRef? = null
    private var resumeEvent: ResumeEvent? = null

    fun offerAndCanRun(event: Event): Boolean = synchronized(this) {
        if (event is ResumeEvent) {
            if (event.cell != blocker) {
                logger.warn("receive event to resume parent, but not from the expected child or not blocked")
                return@synchronized false
            }
            blocker = null
            resumeEvent = event
        } else {
            queue.addLast(event)
        }
        if (consuming || blocker != null) {
            // a worker is running or about to run, or the cell is suspended
            false
        } else {
            // no worker now
            consuming = true
            true
        }
    }

    fun poll(): Event? = synchronized(this) {
        val event = resumeEvent
        if (event != null) {
            resumeEvent = null
            event.result
        } else {
            queue.removeFirst()
        }
    }

    fun hasNext(): Boolean = synchronized(this) {
        // consuming = true
        if (blocker == null) {
            val event = resumeEvent
            if (event != null) {
                if (event.result != VoidEvent) {
                    return@synchronized true
                }
                // no event to pass to suspended parent
                // clear resumeParentEvent and fallback to queue
                resumeEvent = null
            }
            if (queue.isNotEmpty()) {
                return@synchronized true
            }
        }
        /**
         * 1. no blocker
         * 2. queue is empty
         * 3. result of resumeParentEvent is a VoidEvent and queue is empty
         */
        consuming = false
        false
    }

    fun suspendBy(cell: CellRef) {
        synchronized(this) {
            if (blocker != null) {
                logger.warn("failed to set blocker, cell is suspended by a child $blocker now")
            } else {
                blocker = cell
            }
        }
    }
}