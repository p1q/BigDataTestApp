package com.prokop.app.data.big.test.bigdatatest.service;

import com.prokop.app.data.big.test.bigdatatest.model.FileIdentifier;

public interface ParseNotification {
    String getFileUri(String data);

    FileIdentifier getFileIdentifier(String data);
}
