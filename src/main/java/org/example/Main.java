package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class Main {
    static Logger logger = LogManager.getLogger(Main.class);


    public static void main(String[] args) throws IOException, InterruptedException {
        logger.info("Starting....");

        String sendingTopic = "j2rs";
        String receivingTopic = "rs2j";

        SimpleKafkaProducer producer = new SimpleKafkaProducer("simple-java-producer", sendingTopic);
        Thread producerThread = new Thread(producer);
        producerThread.start();

        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer("simple-java-consumer", receivingTopic);
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                // producer.stop();
                consumer.stop();
            }
        });

        // producerThread.join();
        consumerThread.join();
    }
}
