package com.example.kafka.kafka.config;

import com.example.kafka.model.MyMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {
    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Bean
    public ConsumerFactory<String, MyMessage> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                consumerGroupId);

        return new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(), new JacksonJsonDeserializer<>(MyMessage.class));
    }

    @Bean
    public CommonErrorHandler errorHandler() {
        // Retry the batch up to 3 times with a 1-second interval
        FixedBackOff backOff = new FixedBackOff(1000L, 3);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(backOff);

        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
            System.err.printf("Retry attempt %d for record %s due to %s%n",
                    deliveryAttempt, record, ex.getMessage())
        );

        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MyMessage> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MyMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);
        factory.setCommonErrorHandler(errorHandler());

        return factory;
    }
}