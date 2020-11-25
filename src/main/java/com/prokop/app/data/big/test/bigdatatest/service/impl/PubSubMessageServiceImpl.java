package com.prokop.app.data.big.test.bigdatatest.service.impl;

import com.prokop.app.data.big.test.bigdatatest.model.FileIdentifier;
import com.prokop.app.data.big.test.bigdatatest.service.PubSubMessageService;
import org.springframework.stereotype.Service;

import org.json.JSONObject;

@Service
public class PubSubMessageServiceImpl implements PubSubMessageService {
    @Override
    public FileIdentifier getFileIdentifier(String data) {
        final JSONObject jsonObject = new JSONObject(data);
        return new FileIdentifier(jsonObject.getString("name"),
                jsonObject.getString("generation"));
    }
}
