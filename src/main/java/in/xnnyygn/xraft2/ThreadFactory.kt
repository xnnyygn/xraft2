package `in`.xnnyygn.xraft2

import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinWorkerThread
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

class FixedThreadFactory(private val name: String) : ThreadFactory {
    override fun newThread(r: Runnable): Thread {
        return Thread(r, name)
    }
}

class NumberedThreadFactory(private val prefix: String = "worker-") : ThreadFactory {
    private val id = AtomicInteger(0)

    override fun newThread(r: Runnable): Thread {
        return Thread(r, prefix + id.getAndIncrement())
    }
}

class NumberedForkJoinThreadFactory(private val prefix: String = "worker-") : ForkJoinPool.ForkJoinWorkerThreadFactory {
    private val id = AtomicInteger(0)

    override fun newThread(pool: ForkJoinPool?): ForkJoinWorkerThread {
        val worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
        worker.name = (prefix + id.getAndIncrement())
        return worker
    }
}