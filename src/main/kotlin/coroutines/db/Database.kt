package coroutines.db

import com.github.andrewoma.kwery.core.DefaultSession
import com.github.andrewoma.kwery.core.Session
import com.github.andrewoma.kwery.core.dialect.HsqlDialect
import com.github.andrewoma.kwery.core.interceptor.LoggingInterceptor
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import kotlinx.coroutines.experimental.run
import java.sql.Connection
import javax.sql.DataSource
import kotlin.coroutines.experimental.AbstractCoroutineContextElement
import kotlin.coroutines.experimental.CoroutineContext


private class TransactionContext(val connection: Connection, var rollbackOnly: Boolean = false) : AbstractCoroutineContextElement(TransactionContext) {
    companion object Key : CoroutineContext.Key<TransactionContext>
}

private class DataSourceContext(val dataSource: DataSource) : AbstractCoroutineContextElement(DataSourceContext) {
    companion object Key : CoroutineContext.Key<DataSourceContext>
}

fun createDatabaseContext(name: String, poolSize: Int, dataSource: DataSource): CoroutineContext {
    return newFixedThreadPoolContext(nThreads = poolSize, name = name) + DataSourceContext(dataSource)
}

suspend fun <T> transaction(context: CoroutineContext, block: suspend (CoroutineContext) -> T) = run(context) {
    if (context[TransactionContext] == null) {
        newTransaction(context, block)
    } else {
        block(context)
    }
}

var CoroutineContext.rollbackOnly: Boolean
    get() = this[TransactionContext]?.rollbackOnly ?: false
    set(value) {
        val transactionContext = this[TransactionContext]
        if (transactionContext != null) transactionContext.rollbackOnly = value
    }

private val CoroutineContext.dataSource
    get() = this[DataSourceContext]?.dataSource ?: throw IllegalStateException("DataSourceContext not in coroutine scope")

private val CoroutineContext.connection
    get() = this[TransactionContext]?.connection


private suspend fun <T> newTransaction(context: CoroutineContext, block: suspend (CoroutineContext) -> T) {
    val connection = context.dataSource.connection
    try {
        connection.autoCommit = false
        val transactionContext = context + TransactionContext(connection)
        run(transactionContext) {
            block(transactionContext)
        }
        if (transactionContext.rollbackOnly) {
            connection.rollback()
        } else {
            connection.commit()
        }
    } catch (throwable: Throwable) {
        connection.rollback()
    } finally {
        connection.close()
    }
}

suspend fun <T> withConnection(context: CoroutineContext, block: suspend (Connection) -> T) = run(context) {
    val connection = context.connection
    if (connection == null) {
        val newConnection = context.dataSource.connection
        try {
            block(newConnection)
        } finally {
            newConnection.close()
        }
    } else {
        block(connection)
    }
}

suspend fun <T> withSession(context: CoroutineContext, block: suspend (Session) -> T) = withConnection(context) {
    block(DefaultSession(it, HsqlDialect(), interceptor = LoggingInterceptor()))
}

