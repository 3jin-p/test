package org.springframework.batch.aws.athena.reader

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import software.amazon.awssdk.services.athena.AthenaClient
import software.amazon.awssdk.services.athena.model.*

@Component
class DefaultAthenaQueryClient(
    private val athenaClient: AthenaClient,
    private val athenaQueryClientObjectMapper: ObjectMapper,
): AthenaQueryClient {

    private val logger = LoggerFactory.getLogger(this::class.simpleName)

    override fun startQuery(query: String, database: String): String {
        val startQueryExecutionResponse = athenaClient.startQueryExecution { requestBuilder ->
            requestBuilder.queryString(query)
            requestBuilder.queryExecutionContext { contextBuilder ->
                contextBuilder.database(database)
            }
            requestBuilder.workGroup("batch-v3")
        }

        return startQueryExecutionResponse.queryExecutionId()
    }

    override fun waitForQueryToComplete(queryExecutionId: String) {
        val getQueryExecutionRequest = GetQueryExecutionRequest.builder()
            .queryExecutionId(queryExecutionId)
            .build()

        var getQueryExecutionResponse: GetQueryExecutionResponse
        var isQueryStillRunning = true
        var queryState: QueryExecutionState

        while (isQueryStillRunning) {
            getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest)
            queryState = getQueryExecutionResponse.queryExecution().status().state()
            when (queryState) {
                QueryExecutionState.FAILED -> {
                    throw RuntimeException(
                        "Athena Query Failed to run with Error Message: " +
                                getQueryExecutionResponse.queryExecution().status().stateChangeReason()
                    )
                }
                QueryExecutionState.CANCELLED -> throw RuntimeException("Query was cancelled.")
                QueryExecutionState.SUCCEEDED -> isQueryStillRunning = false
                else -> Thread.sleep(SLEEP_AMOUNT_IN_MS)
            }
            logger.debug("Current Status is: {}", queryState)
        }
    }

    override fun <T> processResultRows(query: GetAthenaQueryExecutionRequest, isFirstPage: Boolean, type: TypeReference<T>): GetAthenaQueryExecutionResponse<T> {
        val getQueryResultsRequest = GetQueryResultsRequest.builder()
            .queryExecutionId(query.queryExecutionId)
            .maxResults(query.maxResults)
            .nextToken(query.nextToken)
            .build()
        val getQueryResultsResults = athenaClient.getQueryResults(getQueryResultsRequest)
        if (getQueryResultsResults.resultSet().rows().size < 1) return GetAthenaQueryExecutionResponse(
            queryExecutionId = query.queryExecutionId,
            nextToken = null,
            rows = emptyList()
        )

        val columnInfo = getQueryResultsResults.resultSet().resultSetMetadata().columnInfo()
        val rows = getQueryResultsResults.resultSet().rows()
        val typedRows = rows
            .filterIndexed { index, _ ->
                if (isFirstPage) {
                    index > 0
                } else {
                    true
                }
            }
            .map { row ->
                row.data().mapIndexed { index, column ->
                    val columnName = columnInfo[index].name()
                    val columnValue =
                        column.varCharValue()?.let { typeCastVarCharValue(column.varCharValue(), columnInfo[index]) }
                    columnName to columnValue
                }.toMap()
            }.map {
                athenaQueryClientObjectMapper.convertValue(it, type)
            }

        return GetAthenaQueryExecutionResponse(
            queryExecutionId = query.queryExecutionId,
            nextToken = getQueryResultsResults.nextToken(),
            rows = typedRows
        )
    }

    fun typeCastVarCharValue(varCharValue: String, columnInfo: ColumnInfo): Any = when (columnInfo.type()) {
        "varchar", "date", "timestamp" -> varCharValue
        "tinyint", "smallint", "integer" -> varCharValue.toInt()
        "bigint" -> varCharValue.toLong()
        "double", "decimal" -> if (varCharValue.contains(".")) varCharValue.toDouble() else varCharValue.toLong()
        "float" -> if (varCharValue.contains(".")) varCharValue.toFloat() else varCharValue.toLong()
        "boolean" -> varCharValue.toBoolean()
        "array" -> varCharValue.replace("[", "").replace("]", "").split(", ")
        else -> throw RuntimeException("Unexpected Type is not expected [${columnInfo.type()}]")
    }

    companion object {
        const val SLEEP_AMOUNT_IN_MS = 1000L * 1
    }
}

