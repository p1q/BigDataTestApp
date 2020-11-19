package com.prokop.app.data.big.test.bigdatatest.repository;

public interface AvroRepository {
    void loadAvroFromGCSToBQ(String avroUri, String datasetName, String tableName);
}
