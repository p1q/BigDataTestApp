package com.prokop.app.data.big.test.bigdatatest.repository;

public interface BigQueryRepository {
    void loadAvroFromGCSToBQ(String avroUri, String datasetName, String tableName);
}
