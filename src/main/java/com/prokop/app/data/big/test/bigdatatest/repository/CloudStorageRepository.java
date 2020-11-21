package com.prokop.app.data.big.test.bigdatatest.repository;

import com.prokop.app.data.big.test.bigdatatest.model.FileIdentifier;

public interface CloudStorageRepository {
    byte[] readObjectFromGCS(FileIdentifier fileId, String projectId, String bucketName);
}
