package com.prokop.app.data.big.test.bigdatatest.service;

import com.prokop.app.data.big.test.bigdatatest.util.AvroUtility;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SaveAvroToBigQueryService {
    private ReceiveMessageService receiveMessageService;

    @Autowired
    public void setReceiveMessageService(ReceiveMessageService receiveMessageService) {
        this.receiveMessageService = receiveMessageService;
    }

    public void run(String data) {
        String filePath = receiveMessageService.parseNotificationData(data);
        AvroUtility.readAvroAndSaveToBQ(filePath);
    }
}
