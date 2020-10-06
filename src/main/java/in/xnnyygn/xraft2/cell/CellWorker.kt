package `in`.xnnyygn.xraft2.cell

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.LockSupport
import kotlin.random.Random

class CellWorker(private val group: CellWorkerGroup) : Runnable {
    private val deque = ConcurrentLinkedDeque<CellTask>()
    private var thread: Thread? = null

    @Volatile
    private var parked: Boolean = false

    fun start() {
        thread = Thread(this)
        thread!!.start()
    }

    override fun run() {
        var task: CellTask?
        while (!Thread.interrupted()) {
            task = deque.pollFirst()
            if (task == null) {
                parked = true
                LockSupport.park()
                continue;
            }
            if (task.run() == CellTask.NextState.YES) {
                group.submit(task)
            }
        }
    }

    fun submit(task: CellTask) {
        deque.offerLast(task)
        if (parked) {
            parked = false
            LockSupport.unpark(thread)
        }
    }

    fun stop() {
        thread?.interrupt()
    }
}

class CellWorkerGroup(nWorkers: Int) {
    private val workers = List(nWorkers) { _ -> CellWorker(this) }
    private val random = Random(System.currentTimeMillis())

    constructor() : this(Runtime.getRuntime().availableProcessors())

    fun startAll() {
        for (worker in workers) {
            worker.start()
        }
    }

    fun submit(task: CellTask) {
        workers[random.nextInt(workers.size)].submit(task)
    }

    fun stopAll() {
        for (worker in workers) {
            worker.stop()
        }
    }
}