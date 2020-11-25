package com.prokop.app.data.big.test.bigdatatest.service;

import com.prokop.app.data.big.test.bigdatatest.model.FileIdentifier;
import org.springframework.stereotype.Component;

@Component
public interface PubSubMessageService {
    FileIdentifier getFileIdentifier(String data);
}
