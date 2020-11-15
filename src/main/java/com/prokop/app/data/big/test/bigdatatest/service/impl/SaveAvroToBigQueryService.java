package com.prokop.app.data.big.test.bigdatatest.service.impl;

import com.prokop.app.data.big.test.bigdatatest.model.FileIdentifier;
import com.prokop.app.data.big.test.bigdatatest.service.SaveAvroToBigQuery;
import com.prokop.app.data.big.test.bigdatatest.util.AvroUtility;
import com.prokop.app.data.big.test.bigdatatest.util.IOUtility;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SaveAvroToBigQueryService implements SaveAvroToBigQuery {
    private static final String PROJECT_ID = "pacific-ethos-294816";
    private static final String BUCKET_NAME = "big_data_test_app_bucket";
    private static final String DATASET_NAME = "bigdatatestapp_bigquery_dataset";
    private static final String ALL_FIELDS_TABLE_NAME = "client-all-fields";
    private static final String MANDATORY_FIELDS_TABLE_NAME = "client-mandatory-fields";
    private static final String MANDATORY_SCHEMA = "{\"namespace\": \"example.gcp\", \"type\":"
            + " \"record\", \"name\": \"Client\", \"fields\": [{\"name\": \"id\", \"type\":"
            + " \"long\"}, {\"name\": \"name\", \"type\": \"string\"}, ]}";

    private ParseNotificationService parseNotificationService;

    @Autowired
    public void setParseNotificationService(ParseNotificationService parseNotificationService) {
        this.parseNotificationService = parseNotificationService;
    }

    @Override
    public void run(String data) {
        String fileUri = parseNotificationService.getFileUri(data);
        AvroUtility.loadAvroFromGCSToBQ(fileUri, DATASET_NAME, ALL_FIELDS_TABLE_NAME);

        FileIdentifier fileId = parseNotificationService.getFileIdentifier(data);
        byte[] binaryAvro = IOUtility.readObjectFromGCS(fileId, PROJECT_ID, BUCKET_NAME);
        String stringAvro = AvroUtility.binaryAvroToString(binaryAvro);
        String stringAvroMandatory = AvroUtility.removeOptionalFieldsFromAvro(stringAvro);
        byte[] binaryAvroMandatory = AvroUtility.encodeAvro(stringAvroMandatory, MANDATORY_SCHEMA);
        AvroUtility.writeBytesToBQ(binaryAvroMandatory, DATASET_NAME, MANDATORY_FIELDS_TABLE_NAME);
    }
}
