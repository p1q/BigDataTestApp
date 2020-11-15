package com.prokop.app.data.big.test.bigdatatest.service.impl;

import com.prokop.app.data.big.test.bigdatatest.model.FileIdentifier;
import com.prokop.app.data.big.test.bigdatatest.service.ParseNotification;
import org.springframework.stereotype.Service;

import org.json.JSONObject;

@Service
public class ParseNotificationService implements ParseNotification {
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
