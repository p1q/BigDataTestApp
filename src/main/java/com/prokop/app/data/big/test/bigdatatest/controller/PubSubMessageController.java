package com.prokop.app.data.big.test.bigdatatest.controller;

import com.prokop.app.data.big.test.bigdatatest.exception.LoadFileToBigQueryException;
import com.prokop.app.data.big.test.bigdatatest.exception.ReadFileFromCloudStorageException;
import com.prokop.app.data.big.test.bigdatatest.model.PubSubMessage;
import com.prokop.app.data.big.test.bigdatatest.service.BigQueryService;
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
public class PubSubMessageController {
    private final BigQueryService bigQueryService;

    @Autowired
    public PubSubMessageController(BigQueryService bigQueryService) {
        this.bigQueryService = bigQueryService;
    }

    @PostMapping()
    public ResponseEntity<String> receiveMessage(@RequestBody PubSubMessage pubSubMessage) {
        if (isPubSubMessageNullSafe(pubSubMessage) && !isObjectFolder(pubSubMessage)) {
            try {
                bigQueryService.loadFileToBigQuery(decodeMessageData(pubSubMessage));
            } catch (ReadFileFromCloudStorageException | LoadFileToBigQueryException e) {
                return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
            }
            return new ResponseEntity<>(HttpStatus.OK);
        } else if (isPubSubMessageNullSafe(pubSubMessage) && isObjectFolder(pubSubMessage)) {
            return new ResponseEntity<>(HttpStatus.UNPROCESSABLE_ENTITY);
        } else {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
    }

    private boolean isPubSubMessageNullSafe(PubSubMessage pubSubMessage) {
        return pubSubMessage != null
                && pubSubMessage.message.getData() != null;
    }

    private boolean isObjectFolder(PubSubMessage pubSubMessage) {
        JSONObject jsonObject = new JSONObject(decodeMessageData(pubSubMessage));
        return jsonObject.getString("name").endsWith("/");
    }

    private String decodeMessageData(PubSubMessage pubSubMessage) {
        return new String(Base64.getDecoder()
                .decode(pubSubMessage.message.getData()
                        .getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }
}
