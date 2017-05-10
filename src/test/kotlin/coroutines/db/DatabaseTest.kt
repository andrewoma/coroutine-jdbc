package coroutines.db

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import kotlinx.coroutines.experimental.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.Test
import kotlin.coroutines.experimental.CoroutineContext

class DatabaseTest {

    companion object {
        val dataSource = HikariDataSource(HikariConfig().apply {
            driverClassName = "org.hsqldb.jdbc.JDBCDriver"
            jdbcUrl = "jdbc:hsqldb:mem:coroutine_test"
            maximumPoolSize = 10
            isAutoCommit = true
        })

        val db = createDatabaseContext("dbPool", dataSource.maximumPoolSize, dataSource)
    }

    val dao = MyDao()
    val kate = Actress("Kate", "Beckinsale")
    val heather = Actress("Heather", "Graham")

    @Before fun before() = runBlocking<Unit> {
        withSession(db) { session ->
            session.update("DROP TABLE IF EXISTS actress")
            session.update("CREATE TABLE actress(first_name VARCHAR(255), last_name VARCHAR(255))")
        }
    }

    @Test fun `should access db outside transaction`() = runBlocking<Unit> {
        dao.insert(db, kate)
        assertThat(dao.countActresses(db)).isEqualTo(1)
    }


    @Test fun `should access db inside transaction`() = runBlocking<Unit> {
        transaction(db) { tx ->
            dao.insert(tx, kate)
            assertThat(dao.findActresses(tx).count()).isEqualTo(1)
        }
        assertThat(dao.countActresses(db)).isEqualTo(1)
    }

    @Test fun `should rollback via rollbackOnly inside transaction`() = runBlocking<Unit> {
        transaction(db) { tx ->
            dao.insert(tx, kate)
            assertThat(dao.findActresses(tx).count()).isEqualTo(1)
            tx.rollbackOnly = true
        }
        assertThat(dao.countActresses(db)).isEqualTo(0)
    }

    @Test fun `should rollback via exception inside transaction`() = runBlocking<Unit> {
        try {
            transaction(db) { tx ->
                dao.insert(tx, kate)
                assertThat(dao.findActresses(tx).count()).isEqualTo(1)
                throw IllegalStateException("foo")
            }
        } catch(e: IllegalStateException) {
        }
        assertThat(dao.countActresses(db)).isEqualTo(0)
    }

    @Test fun `should handle concurrent access via common pool`() = runBlocking {
        insertConcurrently(CommonPool)
    }

    @Test fun `should handle concurrent access via thread pool exceed connection pool`() = runBlocking {
        insertConcurrently(newFixedThreadPoolContext(1000, "test"))
    }

    @Test fun `should support nested transactions`() = runBlocking<Unit> {
        transaction(db) { tx1 ->
            dao.insert(tx1, kate)
            transaction(tx1) { tx2 ->
                dao.insert(tx2, heather)
            }
            assertThat(dao.countActresses(tx1)).isEqualTo(2)
        }
        assertThat(dao.countActresses(db)).isEqualTo(2)
    }

    private suspend fun insertConcurrently(launchContext: CoroutineContext) {
        val count = 100_000
        val jobs = IntRange(1, count).map { i ->
            launch(launchContext) {
                dao.insert(db, Actress("$i", "$i"))
            }
        }

        jobs.forEach { it.join() } // wait for all jobs to complete

        assertThat(dao.countActresses(db)).isEqualTo(count)
    }

    data class Actress(val firstName: String, val lastName: String)

    class MyDao {

        suspend fun insert(db: CoroutineContext, actress: Actress) = withSession(db) { session ->
            session.update("INSERT INTO actress(first_name, last_name) VALUES (:first_name, :last_name)", mapOf(
                    "first_name" to actress.firstName,
                    "last_name" to actress.lastName
            ))
            actress
        }

        suspend fun findActresses(db: CoroutineContext) = withSession(db) { session ->
            session.select("SELECT * FROM actress") { row ->
                Actress(row.string("first_name"), row.string("last_name"))
            }
        }

        suspend fun countActresses(db: CoroutineContext) = withSession(db) { session ->
            session.select("SELECT count(*) AS COUNT FROM actress") { row -> row.int("count") }.single()
        }
    }
}