package com.prokop.app.data.big.test.bigdatatest.repository;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.prokop.app.data.big.test.bigdatatest.exception.ReadFileFromCloudStorageException;
import com.prokop.app.data.big.test.bigdatatest.model.FileIdentifier;

public interface CloudStorageRepository {
    Blob readObjectFromGCS(Storage storage, FileIdentifier fileId, String bucketName) throws ReadFileFromCloudStorageException;
}
