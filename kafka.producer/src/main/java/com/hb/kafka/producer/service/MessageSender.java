package com.hb.kafka.producer.service;

import lombok.*;
import lombok.experimental.*;
import lombok.extern.slf4j.*;
import org.springframework.kafka.annotation.*;
import org.springframework.kafka.core.*;
import org.springframework.stereotype.*;

import java.util.*;
import java.util.concurrent.*;

import static com.hb.kafka.producer.config.KafkaProducerConfig.*;

@Slf4j
@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class MessageSender {


    private static final Random RA = new Random();

    private static final List<String> KEYS = new ArrayList<>() {{
        add("FIRST");
        add("MAIN");
        add("MESSAGE");
    }};


    KafkaTemplate<String, String> template;

    public boolean send(String message, String topicName, Integer partition) {
        val key = KEYS.get(RA.nextInt(KEYS.size()));
        val future = template.send(topicName,
                partition == null ? 0 : partition,
                key,
                message);

        try {
            val result = future.get(2, TimeUnit.SECONDS);
            log.info("Successful send to {} by key {} with offset {} to partition {}",
                    result.getProducerRecord().topic(), key, result.getRecordMetadata().offset(),
                    result.getRecordMetadata().partition());
            return true;
        } catch (TimeoutException | ExecutionException | InterruptedException e) {
            log.error("Cannot send message to Kafka Topic {}", TOPIC_NAME, e);
        }
        return false;
    }

    @KafkaListener(topics = TOPIC_NAME,
            topicPartitions = @TopicPartition(
                    topic = TOPIC_NAME,
                    partitionOffsets = @PartitionOffset(
                            partition = "0", initialOffset = "0"
                    )))
    public void consume(String message) {
        log.info("Receive new message! {}", message);
    }


}
