package com.prokop.app.data.big.test.bigdatatest.service.impl;

import com.prokop.app.data.big.test.bigdatatest.model.Client;
import com.prokop.app.data.big.test.bigdatatest.model.FileIdentifier;
import com.prokop.app.data.big.test.bigdatatest.repository.BigQueryRepository;
import com.prokop.app.data.big.test.bigdatatest.repository.CloudStorageRepository;
import com.prokop.app.data.big.test.bigdatatest.service.ParseNotificationService;
import com.prokop.app.data.big.test.bigdatatest.service.SaveAvroToBigQueryService;
import com.prokop.app.data.big.test.bigdatatest.util.AvroUtility;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@Service
public class SaveAvroToBigQueryServiceImpl implements SaveAvroToBigQueryService {
    private static final String PROJECT_ID = "big-data-test-app";
    private static final String BUCKET_NAME = "big-data-test-app-bucket";
    private static final String DATASET_NAME = "big_query_dataset";
    private static final String ALL_FIELDS_TABLE_NAME = "client_all_fields";
    private static final String MANDATORY_FIELDS_TABLE_NAME = "client-mandatory-fields";

    private ParseNotificationService parseNotificationService;
    private BigQueryRepository bigQueryRepository;
    private CloudStorageRepository cloudStorageRepository;

    @Autowired
    public SaveAvroToBigQueryServiceImpl(ParseNotificationService parseNotificationService,
                                         BigQueryRepository bigQueryRepository,
                                         CloudStorageRepository cloudStorageRepository) {
        this.parseNotificationService = parseNotificationService;
        this.bigQueryRepository = bigQueryRepository;
        this.cloudStorageRepository = cloudStorageRepository;
    }

    @Override
    public void run(String data) {
/////////////
        String filePath = "a:\\Avro\\avro-sample.avro";
        try {
            byte[] bytes = Files.readAllBytes(Paths.get(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        File file = new File(filePath);
        try {
            byte[] bytes = Files.readAllBytes(file.toPath());

            AvroUtility.serializeNonOptionalAvro(AvroUtility.deserializeAvro(bytes));

        } catch (IOException e) {
            e.printStackTrace();
        }
/////////////////




        // Get uploaded file's identifiers
        FileIdentifier fileId = parseNotificationService.getFileIdentifier(data);

        // Read uploaded file from the bucket to array of bytes
        byte[] binaryAvro = cloudStorageRepository.readObjectFromGCS(fileId, PROJECT_ID, BUCKET_NAME);

        // Load ALL fields from the file to BigQuery
        bigQueryRepository.loadBytesToBigQuery(binaryAvro, DATASET_NAME, ALL_FIELDS_TABLE_NAME);


System.out.println("ZZZZ105:");

        // Deserialize binary Avro to List of clients
        List<Client> clients = AvroUtility.deserializeAvro(binaryAvro);

        // Serialize List of clients to binary Avro with Clients with non optional fields only
        byte[] binaryAvroNonOptional = AvroUtility.serializeNonOptionalAvro(clients);

        // Load NON OPTIONAL fields from the file to BigQuery
        bigQueryRepository.loadBytesToBigQuery(binaryAvroNonOptional, DATASET_NAME, MANDATORY_FIELDS_TABLE_NAME);
    }
}
