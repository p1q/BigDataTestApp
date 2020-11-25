package com.prokop.app.data.big.test.bigdatatest.service;

import org.springframework.stereotype.Service;

@Service
public interface BigQueryService {
    void loadFileToBigQuery(String data);
}
