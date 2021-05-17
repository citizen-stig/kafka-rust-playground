package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleKafkaProducer extends KafkaBase implements Runnable {
    static Logger logger = LogManager.getLogger(SimpleKafkaProducer.class);
    private final String topic;
    private boolean stop = false;
    private final Properties props;

    public SimpleKafkaProducer(String clientId, String topic) throws IOException {
        this.props = getBasicProperties();
        this.props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.topic = topic;
    }

    @Override
    public void run() {
        logger.info("Start producing to topic: {}", topic);

        Producer<Long, String> producer = new KafkaProducer<>(props);
        long index = 0;

        while (!stop) {
            String value = "Data " + index + LocalDateTime.now();
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(
                topic,
                index,
                value
            );
            try {
                RecordMetadata metadata = producer
                    .send(record)
                    .get();
                logger.info("Record sent with key {} to partition {} with metadata {}",
                    index, metadata.partition(), metadata.offset());
            } catch (ExecutionException | InterruptedException e) {
                logger.error("Error in sending record", e);
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            index++;
        }
        producer.close();
        logger.info("Done producing");
    }

    public void stop() {
        this.stop = true;
    }
}
