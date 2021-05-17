package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Main {


    static Logger logger = LogManager.getLogger(Main.class);


    public static Properties getBasicProperties() throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classloader.getResourceAsStream("kafka.properties");
        InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(streamReader);

        Properties props = new Properties();
        props.load(reader);
        reader.close();
        return props;
    }

    public static Producer<Long, String> createProducer() throws IOException {
        Properties props = getBasicProperties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "java-sample-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static Consumer<Long, String> createConsumer() throws IOException {
        Properties props = getBasicProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "java-sample-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("j2rs"));
        return consumer;
    }

    public static void main(String[] args) throws IOException {
        logger.info("Starting....");

        Producer<Long, String> producer = createProducer();

        for (int index = 0; index < 100; index++) {
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(
                "j2rs",
                (long) index,
                "Data " + index);
            try {
                RecordMetadata metadata = producer
                    .send(record)
                    .get();
                logger.info("Record sent with key {} to partition {} with metadata {}",
                    index, metadata.partition(), metadata.offset());
            } catch (ExecutionException | InterruptedException e) {
                logger.error("Error in sending record", e);
            }
        }
        logger.info("DONE PRODUCING");
        producer.close();

        Consumer<Long, String> consumer = createConsumer();

        int noMessageFound = 0;

        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofSeconds(10));
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > 100)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }

            //print each record.
            consumerRecords.forEach(record -> {
                logger.info("Key={} Value={} partition={}, offset={}",
                    record.key(),
                    record.value(),
                    record.partition(),
                    record.offset());
            });

            // commits the offset of record to broker.
            consumer.commitAsync();
        }
        consumer.close();
    }
}
