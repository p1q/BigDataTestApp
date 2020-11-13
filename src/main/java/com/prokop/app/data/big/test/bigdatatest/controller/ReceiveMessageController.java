package com.prokop.app.data.big.test.bigdatatest.controller;

import com.prokop.app.data.big.test.bigdatatest.model.PubSubMessage;
import com.prokop.app.data.big.test.bigdatatest.service.SaveAvroToBigQueryService;
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
public class ReceiveMessageController {
  private SaveAvroToBigQueryService saveAvroToBigQueryService;

  @Autowired
  public void setSaveAvroToBigQueryService(SaveAvroToBigQueryService saveAvroToBigQueryService) {
    this.saveAvroToBigQueryService = saveAvroToBigQueryService;
  }

  // The method is invoked by a Pub/Sub notification when an object is created in the bucket
  @PostMapping()
  public ResponseEntity receiveMessage(@RequestBody PubSubMessage message) {
    if (message != null && message.getData() != null) {
      String data = new String(Base64.getDecoder().decode(message.getData()
              .getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
      saveAvroToBigQueryService.run(data);
    } else {
      return new ResponseEntity(HttpStatus.BAD_REQUEST);
    }
    return new ResponseEntity(HttpStatus.OK);
  }
}
