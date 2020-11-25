package com.prokop.app.data.big.test.bigdatatest.repository;

import com.google.cloud.storage.Storage;
import com.prokop.app.data.big.test.bigdatatest.model.FileIdentifier;

public interface CloudStorageRepository {
    byte[] readObjectFromGCS(Storage storage, FileIdentifier fileId, String bucketName);
}
