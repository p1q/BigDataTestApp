package com.prokop.app.data.big.test.bigdatatest.util;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;

import java.util.logging.Logger;

public class AvroUtility {
  private static final String DATASET_NAME = "bigdatatestapp_bigquery_dataset";
  private static final String ALL_FIELDS_TABLE_NAME = "client-all-fields";
  private static final Logger logger = Logger.getLogger(AvroUtility.class.getName());

  public static void readAvroAndSaveToBQ(String avroUri) {
    try {
      BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

      TableId tableId = TableId.of(DATASET_NAME, ALL_FIELDS_TABLE_NAME);
      LoadJobConfiguration loadConfig = LoadJobConfiguration.of(tableId, avroUri, FormatOptions.avro());

      Job job = bigQuery.create(JobInfo.of(loadConfig));
      job = job.waitFor();

      if (job.isDone()) {
        logger.info("Avro file was successfully saved to the table " + ALL_FIELDS_TABLE_NAME);
      } else {
        logger.severe("Error while loading to BigQuery: " + job.getStatus().getError());
      }

    } catch (BigQueryException | InterruptedException e) {
      logger.severe("Column not added during load append \n" + e.toString());
    }
  }
}
