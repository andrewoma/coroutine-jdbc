# Kotlin coroutines meets JDBC

An experiment in bridging blocking JDBC calls with [Kotlin coroutines](https://github.com/Kotlin/kotlinx.coroutines/blob/master/coroutines-guide.md).
 
It supports re-entrant transactions allowing services to be composed.

See [DatabaseTest.kt](src/test/kotlin/coroutines/db/DatabaseTest.kt) for examples
and [Database.kt](src/main/kotlin/coroutines/db/Database.kt) for the implementation.
 
### Issues 

- Requires the coroutine context be passed around to handle transactions. 
  This is error prone as it is easy to pass the wrong context resulting in multiple
  transactions.
- The coroutine context is essentially untyped so there's no way of verifying 
  the appropriate context has been passed at compile time.
 
### Credits

- Roman Elizarov for the original idea of the fixed thread pool for bridging  