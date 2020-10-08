package `in`.xnnyygn.xraft2.cell

import `in`.xnnyygn.xraft2.getLogger
import java.util.concurrent.Executors
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class CellSystem {
    companion object {
        @JvmStatic
        private val logger = getLogger(CellSystem::class.java)
    }

    private val workerId = AtomicInteger(0)
    private val workerFactory = ForkJoinPool.ForkJoinWorkerThreadFactory { pool ->
        val worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
        worker.name = ("worker-" + workerId.getAndIncrement())
        worker
    }
    private val executorService = ForkJoinPool(Runtime.getRuntime().availableProcessors(), workerFactory, null, true)
    private val scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "scheduler") }
    private val cellExecutors = mutableListOf<CellExecutor>()

    fun add(cell: Cell) {
        cellExecutors.add(RootCellExecutor(cell, executorService, scheduledExecutorService))
    }

    fun start() {
        logger.debug("start")
        for (executor in cellExecutors) {
            executor.start()
        }
    }

    fun stop() {
        logger.debug("stop")
        for (executor in cellExecutors) {
            executor.stop()
        }
        executorService.shutdown()
        executorService.awaitTermination(3L, TimeUnit.SECONDS)
        scheduledExecutorService.shutdown()
        scheduledExecutorService.awaitTermination(1L, TimeUnit.SECONDS)
    }
}