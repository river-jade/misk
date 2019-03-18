package misk.devmode.behaviors

import com.google.common.base.Stopwatch
import com.google.inject.Inject
import com.google.inject.Provider
import misk.devmode.DevModeBehavior
import misk.hibernate.Transacter
import misk.hibernate.shards
import misk.hibernate.transaction
import misk.jdbc.DataSourceConfig
import misk.jdbc.DataSourceType
import misk.jdbc.map
import misk.logging.getLogger
import java.util.Locale
import kotlin.reflect.KClass

class TruncateTablesBehavior @Inject constructor(
  private val qualifier: KClass<out Annotation>,
  private val config: DataSourceConfig,
  private val transacterProvider: Provider<Transacter>
) : DevModeBehavior {
  // TODO(nb): share this logic w/ TruncateTableServices in misk-hibernate-testing
  private val persistentTables = setOf("schema_version")

  override fun run() {
    val stopwatch = Stopwatch.createStarted()

    val truncatedTableNames = transacterProvider.get().shards().flatMap { shard ->
      transacterProvider.get().transaction(shard) { session ->
        val tableNamesQuery = when (config.type) {
          DataSourceType.MYSQL -> {
            "SELECT table_name FROM information_schema.tables where table_schema='${config.database}'"
          }
          DataSourceType.HSQLDB -> {
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.SYSTEM_TABLES WHERE TABLE_TYPE='TABLE'"
          }
          DataSourceType.VITESS -> {
            "SHOW VSCHEMA TABLES"
          }
        }

        @Suppress("UNCHECKED_CAST") // createNativeQuery returns a raw Query.
        val allTableNames = session.useConnection { c ->
          c.createStatement().use { s ->
            s.executeQuery(tableNamesQuery).map { rs -> rs.getString(1) }
          }
        }

        val truncatedTableNames = mutableListOf<String>()
        session.useConnection { connection ->
          val statement = connection.createStatement()
          for (tableName in allTableNames) {
            if (persistentTables.contains(tableName.toLowerCase(Locale.ROOT))) continue
            if (tableName.endsWith("_seq") || tableName.equals("dual")) continue

            statement.addBatch("DELETE FROM $tableName")
            truncatedTableNames += tableName
          }
          statement.executeBatch()
        }

        return@transaction truncatedTableNames
      }
    }

    if (truncatedTableNames.isNotEmpty()) {
      logger.info {
        "@${qualifier.simpleName} TruncateTablesService truncated ${truncatedTableNames.size} " +
            "tables in $stopwatch"
      }
    }
  }

  private companion object {
    val logger = getLogger<TruncateTablesBehavior>()
  }

}