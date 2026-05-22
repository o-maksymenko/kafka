package com.example.kafka.kafka.producer;

import com.example.kafka.model.MyMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class KafkaProducerImpl implements KafkaProducer {
    @Autowired
    private KafkaTemplate<String, MyMessage> kafkaTemplate;

    @Value(value = "${spring.kafka.topic}")
    private String kafkaTopic;

    @Override
    public void sendMessage(String message) {
        MyMessage myMessage = new MyMessage(message, "the message is "+message);

        CompletableFuture<SendResult<String, MyMessage>> future = kafkaTemplate.send(kafkaTopic, myMessage);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + myMessage +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        myMessage + "] due to : " + ex.getMessage());
            }
        });
    }
}
