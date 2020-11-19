package com.prokop.app.data.big.test.bigdatatest.repository.impl;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.prokop.app.data.big.test.bigdatatest.repository.BigQueryRepository;
import org.springframework.stereotype.Repository;

import java.util.logging.Logger;

@Repository
public class BigQueryRepositoryImpl implements BigQueryRepository {
    private final Logger LOGGER = Logger.getLogger(BigQueryRepositoryImpl.class.getName());

    @Override
    public void loadAvroFromGCSToBQ(String avroUri, String datasetName, String tableName) {
        try {
            BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
            TableId tableId = TableId.of(datasetName, tableName);
            LoadJobConfiguration loadConfig = LoadJobConfiguration.of(tableId, avroUri, FormatOptions.avro());

            Job job = bigQuery.create(JobInfo.of(loadConfig));
            job = job.waitFor();

            if (job.isDone()) {
                LOGGER.info("Avro file was successfully saved to the table " + tableName);
            } else {
                LOGGER.severe("Error while loading to BigQuery: " + job.getStatus().getError());
            }
        } catch (BigQueryException | InterruptedException e) {
            LOGGER.severe("Column not added during load append: " + e.toString());
        }
    }
}
