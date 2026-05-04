package com.example.kafka.kafka.consumer;

import com.example.kafka.model.MyMessage;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @KafkaListener(topics = "${spring.kafka.topic}")
    public void processMessage(MyMessage message) {
        System.out.println("Received message: " + message);
    }
}
