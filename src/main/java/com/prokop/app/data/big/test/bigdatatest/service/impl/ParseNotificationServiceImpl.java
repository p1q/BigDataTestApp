package com.prokop.app.data.big.test.bigdatatest.service.impl;

import com.prokop.app.data.big.test.bigdatatest.model.FileIdentifier;
import org.springframework.stereotype.Service;

import org.json.JSONObject;

@Service
public class ParseNotificationServiceImpl implements com.prokop.app.data.big.test.bigdatatest.service.ParseNotificationService {
    @Override
    public String getFileUri(String data) {
        final JSONObject jsonObject = new JSONObject(data);
        return "gs://" + jsonObject.getString("id")
                .replace("/" + jsonObject.getString("generation"), "");
    }

    @Override
    public FileIdentifier getFileIdentifier(String data) {
        final JSONObject jsonObject = new JSONObject(data);
        return new FileIdentifier(jsonObject.getString("name"),
                jsonObject.getString("generation"));
    }
}
