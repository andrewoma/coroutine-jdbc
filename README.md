# Kotlin coroutines meets JDBC

An experiment in bridging blocking JDBC calls with [Kotlin coroutines](https://github.com/Kotlin/kotlinx.coroutines/blob/master/coroutines-guide.md).
 
It supports re-entrant transactions allowing services to be composed.

See [DatabaseTest.kt](src/test/kotlin/coroutines/db/DatabaseTest.kt) for examples
and [Database.kt](src/main/kotlin/coroutines/db/Database.kt) for the implementation.
 
### Credits

- Roman Elizarov for the original idea of the fixed thread pool for bridging and
  how to obtain the current coroutine context
 