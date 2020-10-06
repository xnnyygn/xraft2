package `in`.xnnyygn.xraft2.cell

import java.util.*

/**
 * A queue for cell.
 * Current implementation is a wrapper class of [LinkedList].
 * All methods are synchronized.
 */
class CellQueue<T> {
    private val queue = LinkedList<T>()

    fun offerAndCount(element: T): Int = synchronized(queue) {
        queue.addLast(element)
        queue.size
    }

    fun removeAndCount(): Int = synchronized(queue) {
        queue.removeFirst()
        queue.size
    }

    fun peek(): T? = synchronized(queue) {
        queue.peekFirst()
    }
}