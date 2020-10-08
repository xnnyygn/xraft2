package `in`.xnnyygn.xraft2.cell

import java.util.concurrent.CountDownLatch

class CellChildSet {
    private var frozen: Boolean = false
    private var countDownLatch: CountDownLatch? = null
    private val children = mutableListOf<CellExecutor>()

    // called from the cell
    fun add(executor: CellExecutor) {
        synchronized(this) {
            require(!frozen) { "cannot start a child from a stopping or stopped cell" }
            children.add(executor)
        }
    }

    fun stopAllAndAwait() {
        val countDownLatch: CountDownLatch
        val targetChildren: List<CellExecutor>
        synchronized(this) {
            require(!frozen) { "child set is stopping" }
            frozen = true
            val size = children.size
            if (size == 0) {
                return
            }
            countDownLatch = CountDownLatch(size)
            this.countDownLatch = countDownLatch
            targetChildren = children.toList() // shallow copy
        }
        for (child in targetChildren) {
            child.stop()
        }
        countDownLatch.await()
    }

    fun remove(executor: CellExecutor) {
        synchronized(this) {
            children.remove(executor)
            if (frozen) {
                // countDownLatch won't be null
                countDownLatch!!.countDown()
            }
        }
    }
}