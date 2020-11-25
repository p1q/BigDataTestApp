package com.prokop.app.data.big.test.bigdatatest.repository.impl;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.prokop.app.data.big.test.bigdatatest.model.FileIdentifier;
import com.prokop.app.data.big.test.bigdatatest.repository.CloudStorageRepository;
import org.springframework.stereotype.Repository;

@Repository
public class CloudStorageRepositoryImpl implements CloudStorageRepository {
    @Override
    public byte[] readObjectFromGCS(Storage storage, FileIdentifier fileId, String bucketName) {
        Blob blob = storage.get(BlobId.of(bucketName, fileId.getName(),
                Long.parseLong(fileId.getGeneration())));
        return blob.getContent(Blob.BlobSourceOption.generationMatch());
    }
}
