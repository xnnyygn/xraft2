package `in`.xnnyygn.xraft2.cell

import `in`.xnnyygn.xraft2.FixedThreadFactory
import `in`.xnnyygn.xraft2.NumberedForkJoinThreadFactory
import `in`.xnnyygn.xraft2.getLogger
import java.util.concurrent.Executors
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit

class CellSystem {
    companion object {
        @JvmStatic
        private val logger = getLogger(CellSystem::class.java)
    }

    private val uncaughtExceptionHandler =
        Thread.UncaughtExceptionHandler { _, t -> logger.warn("uncaught exception", t) }
    private val executorService =
        ForkJoinPool(
            Runtime.getRuntime().availableProcessors(),
            NumberedForkJoinThreadFactory(),
            uncaughtExceptionHandler,
            true
        )
    private val scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(FixedThreadFactory("scheduler"))
    private val cellExecutors = mutableListOf<CellExecutor>()

    fun add(cell: Cell): CellRef {
        val c = RootCellExecutor(cell, executorService, scheduledExecutorService)
        cellExecutors.add(c)
        return c
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
        logger.debug("shutdown workers")
        executorService.shutdown()
        executorService.awaitTermination(3L, TimeUnit.SECONDS)
        logger.debug("shutdown scheduler")
        scheduledExecutorService.shutdown()
        scheduledExecutorService.awaitTermination(1L, TimeUnit.SECONDS)
    }
}