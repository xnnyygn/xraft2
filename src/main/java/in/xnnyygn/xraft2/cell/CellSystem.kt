package `in`.xnnyygn.xraft2.cell

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class CellSystem {
    private val workerGroup = CellWorkerGroup()
    private val scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private val executors = mutableListOf<CellExecutor>()

    fun add(cell: Cell) {
        executors.add(RootCellExecutor(cell, workerGroup, scheduledExecutorService))
    }

    fun start() {
        workerGroup.startAll()
        for (executor in executors) {
            executor.start()
        }
    }

    fun stop() {
        for (executor in executors) {
            executor.stop()
        }
//        workerGroup.stopAll()
        scheduledExecutorService.shutdown()
        scheduledExecutorService.awaitTermination(1L, TimeUnit.SECONDS)
    }
}