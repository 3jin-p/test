package org.springframework.batch.aws.athena.reader

data class GetAthenaQueryExecutionRequest(
    val queryExecutionId: String,
    val nextToken: String? = null,
    val maxResults: Int? = null,
)

data class GetAthenaQueryExecutionResponse<T>(
    val queryExecutionId: String,
    val rows: List<T>,
    val nextToken: String? = null,
)
