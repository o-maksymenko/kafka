package com.example.kafka.kafka.consumer;

import com.example.kafka.model.MyMessage;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class KafkaConsumer {

    @KafkaListener(topics = "${spring.kafka.topic}")
    public void processMessage(List<MyMessage> messages) {
        System.out.println("Received messages: " + Arrays.deepToString(messages.toArray()));

        for(int i = 0; i< messages.size(); i++) {
           try {
               if (messages.get(i).message().equalsIgnoreCase("fail")) {
                   throw new RuntimeException("Simulated processing failure at message index: " + i);
               }
           } catch (Exception e) {
               System.err.println("Failed to process message: {}" + messages.get(i));
               throw new BatchListenerFailedException(e.getMessage(), i);
           }
        }

        System.out.println("Processed messages");
    }
}
