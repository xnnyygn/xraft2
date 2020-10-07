package `in`.xnnyygn.xraft2

import org.slf4j.LoggerFactory

typealias LazyString = () -> String

interface Logger {
    fun debug(msg: String)
    fun debug(msg: String, throwable: Throwable)
    fun debug(msg: LazyString)
    fun debug(throwable: Throwable, msg: LazyString)

    fun info(msg: String)
    fun info(msg: String, throwable: Throwable)
    fun info(msg: LazyString)
    fun info(throwable: Throwable, msg: LazyString)

    fun warn(msg: String)
    fun warn(msg: String, throwable: Throwable)
    fun warn(msg: LazyString)
    fun warn(throwable: Throwable, msg: LazyString)

    fun error(msg: String)
    fun error(msg: String, throwable: Throwable)
    fun error(msg: LazyString)
    fun error(throwable: Throwable, msg: LazyString)
}

fun <T> getLogger(clazz: Class<T>): Logger = Slf4jLogger(LoggerFactory.getLogger(clazz))

class Slf4jLogger(private val delegate: org.slf4j.Logger) : Logger {
    override fun debug(msg: String) {
        delegate.debug(msg)
    }

    override fun debug(msg: String, throwable: Throwable) {
        delegate.debug(msg, throwable)
    }

    override fun debug(msg: LazyString) {
        if (delegate.isDebugEnabled) {
            delegate.debug(msg())
        }
    }

    override fun debug(throwable: Throwable, msg: LazyString) {
        if (delegate.isDebugEnabled) {
            delegate.debug(msg(), throwable)
        }
    }

    override fun info(msg: String) {
        delegate.info(msg)
    }

    override fun info(msg: String, throwable: Throwable) {
        delegate.info(msg, throwable)
    }

    override fun info(msg: LazyString) {
        if (delegate.isInfoEnabled) {
            delegate.info(msg())
        }
    }

    override fun info(throwable: Throwable, msg: LazyString) {
        if (delegate.isInfoEnabled) {
            delegate.info(msg(), throwable)
        }
    }

    override fun warn(msg: String) {
        delegate.warn(msg)
    }

    override fun warn(msg: String, throwable: Throwable) {
        delegate.warn(msg, throwable)
    }

    override fun warn(msg: LazyString) {
        if (delegate.isWarnEnabled) {
            delegate.warn(msg())
        }
    }

    override fun warn(throwable: Throwable, msg: LazyString) {
        if (delegate.isWarnEnabled) {
            delegate.warn(msg(), throwable)
        }
    }

    override fun error(msg: String) {
        delegate.error(msg)
    }

    override fun error(msg: String, throwable: Throwable) {
        delegate.error(msg, throwable)
    }

    override fun error(msg: LazyString) {
        if (delegate.isErrorEnabled) {
            delegate.error(msg())
        }
    }

    override fun error(throwable: Throwable, msg: LazyString) {
        if (delegate.isErrorEnabled) {
            delegate.error(msg(), throwable)
        }
    }
}