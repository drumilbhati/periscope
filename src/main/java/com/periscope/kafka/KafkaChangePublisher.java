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
        
        // TODO 1: Set the bootstrap servers from the config
        // Hint: props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());

        // TODO 2: Set strict delivery guarantees (acks=all and enable idempotence)
        // Hint: props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Hint: props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // TODO 3: Configure serializers. Key is String, Value is byte[].
        // Hint: props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Hint: props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // TODO 4: Initialize the 'producer' field with the configured properties
        // Hint: this.producer = new KafkaProducer<>(props);
        this.producer = new KafkaProducer<>(props);
    }

    public java.util.concurrent.CompletableFuture<Void> publish(String topic, String key, com.periscope.model.ChangeEvent event) {
        // TODO 5: Use 'eventSerializer' to convert the event to a byte[]
        byte[] data = eventSerializer.serialize(event);

        // TODO 6: Create a new ProducerRecord<String, byte[]>(topic, key, serializedEventBytes)
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, data);

        // TODO 7: Create a new CompletableFuture<Void> called 'future'
        CompletableFuture<Void> future = new CompletableFuture<>();

        // TODO 8: Send the record asynchronously. 
        // Hint: producer.send(record, (metadata, exception) -> {
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

        // TODO 9: Return the future
        return future;
    }

    @Override
    public void close() {
        // TODO 10: Flush the producer to ensure any buffered messages are sent to the network.
        // Hint: producer.flush();
        producer.flush();

        // TODO 11: Close the producer to release resources.
        // Hint: producer.close();
        producer.close();
    }
}
