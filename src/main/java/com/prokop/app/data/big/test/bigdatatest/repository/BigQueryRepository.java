package com.prokop.app.data.big.test.bigdatatest.repository;

public interface BigQueryRepository {
    void loadBytesToBigQuery(byte[] binaryAvro, String datasetName, String tableName);
}
