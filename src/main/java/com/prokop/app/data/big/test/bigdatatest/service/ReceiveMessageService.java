package com.prokop.app.data.big.test.bigdatatest.service;

import org.springframework.stereotype.Service;

import org.json.JSONObject;

@Service
public class ReceiveMessageService {
    public String parseNotificationData(String data) {
        final JSONObject jsonObject = new JSONObject(data);
        return "gs://" + jsonObject.getString("id")
                .replace("/" + jsonObject.getString("generation"), "");
    }
}
