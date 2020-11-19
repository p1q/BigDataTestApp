package com.prokop.app.data.big.test.bigdatatest.controller;

import com.prokop.app.data.big.test.bigdatatest.model.PubSubMessage;
import com.prokop.app.data.big.test.bigdatatest.service.impl.SaveAvroToBigQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

@RestController
@RequestMapping("/")
public class ReceiveNotificationController {
    private SaveAvroToBigQueryService saveAvroToBigQueryService;

    @Autowired
    public void setSaveAvroToBigQueryService(SaveAvroToBigQueryService saveAvroToBigQueryService) {
        this.saveAvroToBigQueryService = saveAvroToBigQueryService;
    }

    // The method is invoked by a Pub/Sub notification when an object is successfully created in the bucket
    @PostMapping()
    public ResponseEntity receiveMessage(@RequestBody PubSubMessage pubSubMessage) {
        if (pubSubMessage != null && pubSubMessage.message.getData() != null) {
            String data = new String(Base64.getDecoder().decode(pubSubMessage.message.getData()
                    .getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
            saveAvroToBigQueryService.run(data);
        } else {
            return new ResponseEntity(HttpStatus.BAD_REQUEST);
        }
        return new ResponseEntity(HttpStatus.OK);
    }
}
