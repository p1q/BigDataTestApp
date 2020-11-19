package com.prokop.app.data.big.test.bigdatatest.service.impl;

import com.prokop.app.data.big.test.bigdatatest.repository.BigQueryRepository;
import com.prokop.app.data.big.test.bigdatatest.service.ParseNotificationService;
import com.prokop.app.data.big.test.bigdatatest.service.SaveAvroToBigQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

@Service
public class SaveAvroToBigQueryServiceImpl implements SaveAvroToBigQueryService {
    private static final String PROJECT_ID = "big-data-test-app";
    private static final String BUCKET_NAME = "big-data-test-app-bucket";
    private static final String DATASET_NAME = "big_query_dataset";
    private static final String ALL_FIELDS_TABLE_NAME = "client_all_fields";
    private static final String MANDATORY_FIELDS_TABLE_NAME = "client-mandatory-fields";

    private final Logger LOGGER = Logger.getLogger(SaveAvroToBigQueryServiceImpl.class.getName());

    private ParseNotificationService parseNotificationService;
    private BigQueryRepository bigQueryRepository;

    @Autowired
    public void setParseNotificationService(ParseNotificationService parseNotificationService,
                                            BigQueryRepository bigQueryRepository) {
        this.parseNotificationService = parseNotificationService;
        this.bigQueryRepository = bigQueryRepository;
    }

    @Override
    public void run(String data) {

System.out.println("ZZZZ48:");

        String fileUri = parseNotificationService.getFileUri(data);

        System.out.println("FileUri: " + fileUri);

        bigQueryRepository.loadAvroFromGCSToBQ(fileUri, DATASET_NAME, ALL_FIELDS_TABLE_NAME);

/*
        FileIdentifier fileId = parseNotificationService.getFileIdentifier(data);

        byte[] binaryAvro = IOUtility.readObjectFromGCS(fileId, PROJECT_ID, BUCKET_NAME);
        String stringAvro = AvroUtility.binaryAvroToString(binaryAvro);
        String stringAvroMandatory = AvroUtility.removeOptionalFieldsFromAvro(stringAvro);
        byte[] binaryAvroMandatory = AvroUtility.encodeAvro(stringAvroMandatory, MANDATORY_SCHEMA);
        AvroUtility.writeBytesToBQ(binaryAvroMandatory, DATASET_NAME, MANDATORY_FIELDS_TABLE_NAME);

 */
    }
}
