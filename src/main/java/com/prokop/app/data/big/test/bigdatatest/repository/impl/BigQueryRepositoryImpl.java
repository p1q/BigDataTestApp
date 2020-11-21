package com.prokop.app.data.big.test.bigdatatest.repository.impl;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import com.prokop.app.data.big.test.bigdatatest.repository.BigQueryRepository;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.logging.Logger;

@Repository
public class BigQueryRepositoryImpl implements BigQueryRepository {
    private final Logger LOGGER = Logger.getLogger(BigQueryRepositoryImpl.class.getName());

    public void loadBytesToBigQuery(byte[] binaryAvro, String datasetName, String tableName) {
        BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
        TableId tableId = TableId.of(datasetName, tableName);
        WriteChannelConfiguration writeChannelConfiguration =
                WriteChannelConfiguration.newBuilder(tableId)
                        .setFormatOptions(FormatOptions.avro())
                        .build();
        TableDataWriteChannel writer = bigQuery.writer(writeChannelConfiguration);

        try (OutputStream stream = Channels.newOutputStream(writer)) {
            stream.write(binaryAvro);
        } catch (IOException e) {
            LOGGER.severe("Input/Output error: " + e.toString());
        }

        Job job = writer.getJob();
        try {
            job = job.waitFor();
        } catch (InterruptedException e) {
            LOGGER.severe("Current thread was interrupted while waiting for the job to complete: "
                    + e.toString());
        }

        if (job.isDone()) {
            LOGGER.info("Avro file was successfully saved to the table " + tableName);
        } else {
            LOGGER.severe("Error while loading to BigQuery: " + job.getStatus().getError());
        }
    }
}
