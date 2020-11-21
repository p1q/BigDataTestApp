package com.prokop.app.data.big.test.bigdatatest.controller;

import com.prokop.app.data.big.test.bigdatatest.model.PubSubMessage;
import com.prokop.app.data.big.test.bigdatatest.service.impl.SaveAvroToBigQueryServiceImpl;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

@RestController
@RequestMapping("/")
public class ReceiveNotificationController {
    private SaveAvroToBigQueryServiceImpl saveAvroToBigQueryServiceImpl;

    @Autowired
    public void setSaveAvroToBigQueryServiceImpl(SaveAvroToBigQueryServiceImpl saveAvroToBigQueryServiceImpl) {
        this.saveAvroToBigQueryServiceImpl = saveAvroToBigQueryServiceImpl;
    }

    // The method is invoked by a Pub/Sub notification when an object is successfully created in the bucket
    @PostMapping()
    public ResponseEntity receiveMessage(@RequestBody PubSubMessage pubSubMessage) {
        if (pubSubMessage != null && pubSubMessage.message.getData() != null) {
            String data = new String(Base64.getDecoder().decode(pubSubMessage.message.getData()
                    .getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);

            // Check, whether it's a "folder" in the bucket
            final JSONObject jsonObject = new JSONObject(data);
            if (jsonObject.getString("name").substring(jsonObject.getString("name")
                    .length() - 1).equals("/")) {
                return new ResponseEntity(HttpStatus.BAD_REQUEST);
            } else {
                // Start Avro loading
                saveAvroToBigQueryServiceImpl.run(data);
            }
        } else {
            return new ResponseEntity(HttpStatus.BAD_REQUEST);
        }
        return new ResponseEntity(HttpStatus.OK);
    }
}
