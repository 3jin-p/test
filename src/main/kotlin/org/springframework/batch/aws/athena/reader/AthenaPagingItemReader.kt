package org.springframework.batch.aws.athena.reader

import com.fasterxml.jackson.core.type.TypeReference
import org.springframework.batch.item.database.AbstractPagingItemReader
import java.util.concurrent.CopyOnWriteArrayList

open class AthenaPagingItemReader<T>(
    val query: String,
    val dataBase: String,
    val size: Int = 1,
    val athenaQueryClient: AthenaQueryClient,
    val type: TypeReference<T>,
) : AbstractPagingItemReader<T>() {

    @Volatile var isFirstPage = true
        set(value) {
            synchronized(this) {
                field = value
            }
        }
    @Volatile var nextToken: String? = null
        set(value) {
            synchronized(this) {
                field = value
            }
        }

    var executionId: String? = null

    init {
        super.setPageSize(size)
    }

    override fun doOpen() {
        executionId = loadDataStream()
        super.doOpen()
    }

    override fun doClose() {
        isFirstPage = true
        nextToken = null
        executionId = null
        super.doClose()
    }

    override fun doReadPage() {
        initializeResults()

        var pageSizeForRequest = pageSize

        val isFirstPageSnapShot = isFirstPage
        val nextTokenSnapShot = nextToken

        if (isFirstPageSnapShot) {
            pageSizeForRequest += 1
        }

        val result = athenaQueryClient.processResultRows(
            GetAthenaQueryExecutionRequest(executionId!!, nextToken = nextTokenSnapShot, maxResults = pageSizeForRequest),
            isFirstPage = isFirstPageSnapShot,
            type
        )

        val rows = result.rows

        nextToken = result.nextToken
        isFirstPage = false

        results.addAll(rows)
    }

    override fun doJumpToPage(itemIndex: Int) {}

    private fun initializeResults() {
        if (results.isNullOrEmpty()) {
            results = CopyOnWriteArrayList()
        } else {
            results.clear()
        }
    }

    private fun loadDataStream(): String {
        athenaQueryClient.startQuery(query, dataBase)
        val executionId = athenaQueryClient.startQuery(query, dataBase)
        athenaQueryClient.waitForQueryToComplete(executionId)
        return executionId
    }
}
