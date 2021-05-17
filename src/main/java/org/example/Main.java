package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class Main {
    static Logger logger = LogManager.getLogger(Main.class);


    public static void main(String[] args) throws IOException, InterruptedException {
        logger.info("Starting....");

        String topic = "j2rs";

        SimpleKafkaProducer producer = new SimpleKafkaProducer("simple-java-producer", topic);
        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer("simple-java-consumer", topic);

        Thread producerThread = new Thread(producer);
        producerThread.start();

        Thread consumerThread = new Thread(consumer);

        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                producer.stop();
                consumer.stop();
            }
        });

        producerThread.join();
    }
}
