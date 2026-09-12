package com.periscope.kafka;

import com.periscope.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class KafkaChangePublisher implements AutoCloseable {

    // We use <String, byte[]> because the partition key is a String, 
    // and our EventSerializer outputs a byte array.
    private final KafkaProducer<String, byte[]> producer;
    private final EventSerializer eventSerializer;

    public KafkaChangePublisher(KafkaConfig config, EventSerializer serializer) {
        this.eventSerializer = serializer;
        
        Properties props = new Properties();
        
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    public java.util.concurrent.CompletableFuture<Void> publish(String topic, String key, com.periscope.model.ChangeEvent event) {
        byte[] data = eventSerializer.serialize(event);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, data);

        CompletableFuture<Void> future = new CompletableFuture<>();

        //           if (exception != null) {
        //               future.completeExceptionally(exception);
        //           } else {
        //               future.complete(null);
        //           }
        //       });
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                future.completeExceptionally(exception);
            } else {
                future.complete(null);
            }
        });

        return future;
    }

    @Override
    public void close() {
        producer.flush();

        producer.close();
    }
}
