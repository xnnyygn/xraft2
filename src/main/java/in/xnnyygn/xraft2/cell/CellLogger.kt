package `in`.xnnyygn.xraft2.cell

import `in`.xnnyygn.xraft2.LazyString
import `in`.xnnyygn.xraft2.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MarkerFactory

class CellLogger(private val name: String, clazz: Class<*>) : Logger {
    companion object {
        private val marker = MarkerFactory.getMarker("CELL")
    }

    private val delegate = LoggerFactory.getLogger(clazz)

    override val debugEnabled: Boolean
        get() = delegate.isDebugEnabled

    override fun debug(msg: String) {
        if (delegate.isDebugEnabled) {
            delegate.debug(marker, concat(msg))
        }
    }

    private fun concat(msg: String) = "$name - $msg"

    override fun debug(msg: String, throwable: Throwable) {
        if (delegate.isDebugEnabled) {
            delegate.debug(marker, concat(msg), throwable)
        }
    }

    override fun debug(msg: LazyString) {
        if (delegate.isDebugEnabled) {
            delegate.debug(marker, concat(msg()))
        }
    }

    override fun debug(throwable: Throwable, msg: LazyString) {
        if (delegate.isDebugEnabled) {
            delegate.debug(marker, concat(msg()), throwable)
        }
    }

    override val infoEnabled: Boolean
        get() = delegate.isInfoEnabled

    override fun info(msg: String) {
        if (delegate.isInfoEnabled) {
            delegate.info(marker, concat(msg))
        }
    }

    override fun info(msg: String, throwable: Throwable) {
        if (delegate.isInfoEnabled) {
            delegate.info(marker, concat(msg), throwable)
        }
    }

    override fun info(msg: LazyString) {
        if (delegate.isInfoEnabled) {
            delegate.info(marker, concat(msg()))
        }
    }

    override fun info(throwable: Throwable, msg: LazyString) {
        if (delegate.isInfoEnabled) {
            delegate.info(marker, concat(msg()), throwable)
        }
    }

    override val warnEnabled: Boolean
        get() = delegate.isWarnEnabled

    override fun warn(msg: String) {
        if (delegate.isWarnEnabled) {
            delegate.warn(marker, concat(msg))
        }
    }

    override fun warn(msg: String, throwable: Throwable) {
        if (delegate.isWarnEnabled) {
            delegate.warn(marker, concat(msg), throwable)
        }
    }

    override fun warn(msg: LazyString) {
        if (delegate.isWarnEnabled) {
            delegate.warn(marker, concat(msg()))
        }
    }

    override fun warn(throwable: Throwable, msg: LazyString) {
        if (delegate.isWarnEnabled) {
            delegate.warn(marker, concat(msg()), throwable)
        }
    }

    override val errorEnabled: Boolean
        get() = delegate.isErrorEnabled

    override fun error(msg: String) {
        if (delegate.isErrorEnabled) {
            delegate.error(marker, concat(msg))
        }
    }

    override fun error(msg: String, throwable: Throwable) {
        if (delegate.isErrorEnabled) {
            delegate.error(marker, concat(msg), throwable)
        }
    }

    override fun error(msg: LazyString) {
        if (delegate.isErrorEnabled) {
            delegate.error(marker, concat(msg()))
        }
    }

    override fun error(throwable: Throwable, msg: LazyString) {
        if (delegate.isErrorEnabled) {
            delegate.error(marker, concat(msg()), throwable)
        }
    }
}