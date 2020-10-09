package `in`.xnnyygn.xraft2.cell

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class CellSystemTest {
    class SchedulerCell(private val countDownLatch: CountDownLatch) : Cell() {
        override fun start(context: CellContext) {
            context.schedule(100L, TimeUnit.MILLISECONDS, PrintEvent)
        }

        override fun receive(context: CellContext, event: CellEvent) {
            if (event == PrintEvent) {
                context.logger.info("hello")
                countDownLatch.countDown()
            }
        }
    }

    object PrintEvent : CellEvent {
        override fun toString() = "PrintMessage"
    }

    @Test
    @Disabled("time consuming")
    fun testScheduler() {
        val countDownLatch = CountDownLatch(1)
        val system = CellSystem()
        system.add(SchedulerCell(countDownLatch))
        system.start()
        countDownLatch.await()
        system.stop()
    }

    class ParentCell(private val countDownLatch: CountDownLatch) : Cell() {
        override fun start(context: CellContext) {
            context.startChild(ChildCell())
        }

        override fun receive(context: CellContext, event: CellEvent) {
            if (event == ChildStartedEvent) {
                context.logger.info("child started")
                countDownLatch.countDown()
            }
        }

        override fun stop(context: CellContext) {
            context.logger.info("stop parent")
        }
    }

    class ChildCell : Cell() {
        override fun start(context: CellContext) {
            context.parent.tell(ChildStartedEvent)
        }

        override fun receive(context: CellContext, event: CellEvent) {
        }

        override fun stop(context: CellContext) {
            context.logger.info("stop child")
        }
    }

    object ChildStartedEvent : CellEvent {
        override fun toString() = "ChildStartedMessage"
    }

    @Test
    fun testParentChild() {
        val countDownLatch = CountDownLatch(1)
        val system = CellSystem()
        system.add(ParentCell(countDownLatch))
        system.start()
        countDownLatch.await()
        system.stop()
    }
}