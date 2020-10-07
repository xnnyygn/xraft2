package `in`.xnnyygn.xraft2.cell

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class CellSystem {
    private val executorService = Executors.newWorkStealingPool()
    private val scheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
    private val cellExecutors = mutableListOf<CellExecutor>()

    fun add(cell: Cell) {
        cellExecutors.add(RootCellExecutor(cell, executorService, scheduledExecutorService))
    }

    fun start() {
        for (executor in cellExecutors) {
            executor.start()
        }
    }

    fun stop() {
        for (executor in cellExecutors) {
            executor.stop()
        }
        executorService.shutdown()
        executorService.awaitTermination(3L, TimeUnit.SECONDS)
        scheduledExecutorService.shutdown()
        scheduledExecutorService.awaitTermination(1L, TimeUnit.SECONDS)
    }
}