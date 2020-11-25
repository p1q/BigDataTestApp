package com.prokop.app.data.big.test.bigdatatest.service.impl;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.prokop.app.data.big.test.bigdatatest.model.Client;
import com.prokop.app.data.big.test.bigdatatest.model.FileIdentifier;
import com.prokop.app.data.big.test.bigdatatest.repository.BigQueryRepository;
import com.prokop.app.data.big.test.bigdatatest.repository.CloudStorageRepository;
import com.prokop.app.data.big.test.bigdatatest.service.PubSubMessageService;
import com.prokop.app.data.big.test.bigdatatest.service.BigQueryService;
import com.prokop.app.data.big.test.bigdatatest.util.AvroUtility;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class BigQueryServiceImpl implements BigQueryService {
    private static final String PROJECT_ID = "big-data-test-app";
    private static final String BUCKET_NAME = "big-data-test-app-bucket";
    private static final String DATASET_NAME = "big_query_dataset";
    private static final String ALL_FIELDS_TABLE_NAME = "client_all_fields";
    private static final String MANDATORY_FIELDS_TABLE_NAME = "client_mandatory_fields";

    private final PubSubMessageService pubSubMessageService;
    private final BigQueryRepository bigQueryRepository;
    private final CloudStorageRepository cloudStorageRepository;

    @Autowired
    public BigQueryServiceImpl(PubSubMessageService pubSubMessageService,
                               BigQueryRepository bigQueryRepository,
                               CloudStorageRepository cloudStorageRepository) {
        this.pubSubMessageService = pubSubMessageService;
        this.bigQueryRepository = bigQueryRepository;
        this.cloudStorageRepository = cloudStorageRepository;
    }

    @Override
    public void loadFileToBigQuery(String data) {
        // Get uploaded file's identifiers
        FileIdentifier fileId = pubSubMessageService.getFileIdentifier(data);

        // Read uploaded file from the bucket to array of bytes
        Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();
        byte[] binaryAvro = cloudStorageRepository.readObjectFromGCS(storage, fileId, BUCKET_NAME);

        // Deserialize binary Avro to List of clients
        List<Client> clients = AvroUtility.deserializeAvro(binaryAvro);

        // Serialize List of clients to binary Avro with Clients with non optional fields only
        byte[] binaryAvroNonOptional = AvroUtility.serializeNonOptionalAvro(clients);

        // Load ALL fields from the file to BigQuery
        bigQueryRepository.loadBytesToBigQuery(binaryAvro, DATASET_NAME, ALL_FIELDS_TABLE_NAME);

        // Load NON OPTIONAL fields from the file to BigQuery
        bigQueryRepository.loadBytesToBigQuery(binaryAvroNonOptional, DATASET_NAME, MANDATORY_FIELDS_TABLE_NAME);
    }
}
