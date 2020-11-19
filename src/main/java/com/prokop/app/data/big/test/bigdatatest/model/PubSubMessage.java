package com.prokop.app.data.big.test.bigdatatest.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PubSubMessage {
    public Message message;
    public String subscription;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public class Message {
        public String data;
        public String message_id;
        public String messageId;
        public Date publish_time;
        public Date publishTime;
        private Map<String, String> attributes;
    }
}
