package com.prokop.app.data.big.test.bigdatatest.repository;

import com.google.cloud.bigquery.BigQuery;
import com.prokop.app.data.big.test.bigdatatest.exception.LoadFileToBigQueryException;

public interface BigQueryRepository {
    void loadBytesToBigQuery(byte[] binaryAvro, String datasetName, String tableName, BigQuery bigQuery)
            throws LoadFileToBigQueryException;
}
