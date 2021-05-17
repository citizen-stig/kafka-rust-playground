package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleKafkaConsumer extends KafkaBase implements Runnable {
    static Logger logger = LogManager.getLogger(SimpleKafkaConsumer.class);
    private final String topic;
    private boolean stop = false;
    private final Properties props;

    public SimpleKafkaConsumer(String clientId, String topic) throws IOException {
        this.props = getBasicProperties();
        this.props.put(ConsumerConfig.GROUP_ID_CONFIG, clientId);
        this.props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        this.props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        this.props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.topic = topic;
    }


    @Override
    public void run() {
        logger.info("Starting consumer from topic {}", topic);
        Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        int noMessageFound = 0;

        while (!stop) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofSeconds(10));
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > 100)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }
            consumerRecords.forEach(record -> {
                logger.info("Key={} Value='{}' partition={}, offset={}",
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

    public void stop() {
        this.stop = true;
    }
}
