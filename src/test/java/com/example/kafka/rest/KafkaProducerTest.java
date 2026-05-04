package com.example.kafka.rest;

import com.example.kafka.kafka.producer.KafkaProducerImpl;
import com.example.kafka.model.MyMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaProducerTest {
    @InjectMocks
    private KafkaProducerImpl producer;

    @Mock
    private KafkaTemplate<String, MyMessage> kafkaTemplate;

    @Mock
    CompletableFuture<SendResult<String, MyMessage>> future;

    @Test
    void sendMessage() {
        String message = "test";
        MyMessage myMessage = new MyMessage(message, "the message is "+message);

        when(kafkaTemplate.send(any(), any(MyMessage.class)))
                .thenReturn(future);

        producer.sendMessage(message);
// the topic is null, cause of mocking the producer and not using it from the application context
        Mockito.verify(kafkaTemplate, Mockito.times(1))
                .send(null, myMessage);
    }
}