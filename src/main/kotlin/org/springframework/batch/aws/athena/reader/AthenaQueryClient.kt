package org.springframework.batch.aws.athena.reader

import com.fasterxml.jackson.core.type.TypeReference

interface AthenaQueryClient {

    fun startQuery(query: String, database: String): String
    fun waitForQueryToComplete(queryExecutionId: String)
    fun <T> processResultRows(query: GetAthenaQueryExecutionRequest, isFirstPage: Boolean, type: TypeReference<T>): GetAthenaQueryExecutionResponse<T>
}
