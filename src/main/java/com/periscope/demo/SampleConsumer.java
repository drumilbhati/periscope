package com.periscope.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Small downstream consumer used by the local demo and getting-started guide.
 *
 * Usage:
 *   SampleConsumer [bootstrapServers] [groupId] [topic]
 */
public final class SampleConsumer {

    private static final Logger log = LoggerFactory.getLogger(SampleConsumer.class);
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_GROUP_ID = "periscope-sample-consumer";
    private static final String DEFAULT_TOPIC = "periscope.public.customers";

    private SampleConsumer() {
    }

    public static void main(String[] args) {
        String bootstrapServers = argument(args, 0, DEFAULT_BOOTSTRAP_SERVERS);
        String groupId = argument(args, 1, DEFAULT_GROUP_ID);
        String topic = argument(args, 2, DEFAULT_TOPIC);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        ObjectMapper mapper = new ObjectMapper();
        AtomicBoolean running = new AtomicBoolean(true);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                running.set(false);
                consumer.wakeup();
            }, "periscope-sample-consumer-shutdown"));

            consumer.subscribe(List.of(topic));
            log.info("Sample consumer subscribed to topic={} group={} bootstrapServers={}",
                    topic, groupId, bootstrapServers);

            try {
                while (running.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logEvent(mapper, record);
                    }
                }
            } catch (WakeupException wakeup) {
                if (running.get()) {
                    throw wakeup;
                }
            }
        }

        log.info("Sample consumer stopped");
    }

    private static void logEvent(ObjectMapper mapper, ConsumerRecord<String, String> record) {
        try {
            JsonNode event = mapper.readTree(record.value());
            log.info("Received topic={} partition={} offset={} key={} operation={} lsn={}",
                    record.topic(), record.partition(), record.offset(), record.key(),
                    event.path("operation").asText("UNKNOWN"), event.path("lsn").asLong(-1));
        } catch (Exception e) {
            log.warn("Received malformed change event at topic={} partition={} offset={}: {}",
                    record.topic(), record.partition(), record.offset(), e.getMessage());
        }
    }

    private static String argument(String[] args, int index, String defaultValue) {
        return args.length > index && !args[index].isBlank() ? args[index] : defaultValue;
    }
}
