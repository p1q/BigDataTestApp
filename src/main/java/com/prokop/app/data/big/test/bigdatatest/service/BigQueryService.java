package com.prokop.app.data.big.test.bigdatatest.service;

import com.prokop.app.data.big.test.bigdatatest.exception.LoadFileToBigQueryException;
import com.prokop.app.data.big.test.bigdatatest.exception.ReadFileFromCloudStorageException;
import org.springframework.stereotype.Service;

@Service
public interface BigQueryService {
    void loadFileToBigQuery(String data) throws ReadFileFromCloudStorageException, LoadFileToBigQueryException;
}
