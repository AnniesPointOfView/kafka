package com.hb.kafka.producer.config;

import lombok.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.*;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.*;

import java.util.*;

@Configuration
public class KafkaProducerConfig {

    public static final String TOPIC_NAME = "kafka.example.topic";
    public static final String TOPIC_NAME_MESSAGE = TOPIC_NAME + ".message";
    public static final String TOPIC_NAME_BOOKING =  TOPIC_NAME + ".booking";

    @Value("${spring.kafka.bootstrap-servers}")
    private String serverAddress;

    @Bean
    public NewTopic kafkaExampleTopic() {
        return TopicBuilder.name(TOPIC_NAME)
                .replicas(1)
                .partitions(10)
                .build();
    }

    @Bean
    public NewTopic kafkaMessageTopic() {
        return TopicBuilder.name(TOPIC_NAME_MESSAGE)
                .replicas(1)
                .partitions(10)
                .build();
    }

    @Bean
    public NewTopic kafkaBookingTopic() {
        return TopicBuilder.name(TOPIC_NAME_BOOKING)
                .replicas(1)
                .partitions(10)
                .build();
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {

        val config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddress);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaStringTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

}
