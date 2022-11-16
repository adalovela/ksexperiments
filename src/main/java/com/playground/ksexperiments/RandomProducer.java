package com.playground.ksexperiments;

import com.github.javafaker.Faker;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@RequiredArgsConstructor
public class RandomProducer {

    private static final Logger logger = LoggerFactory.getLogger(RandomProducer.class);

    private static String inputTopic;

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        try (InputStream inputStream = Files.newInputStream(Paths.get(args[0]))) {
            props.load(inputStream);
            inputTopic = props.getProperty("input.topic.name");
            TopicsManager bootstrapper = new TopicsManager(props);
            bootstrapper.createTopics(Collections.singleton(new NewTopic(inputTopic, Optional.empty(), Optional.empty())));
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            start(producer, inputTopic);
        }
    }

    private static void start(KafkaProducer<String, String> producer, String topic) {
        Faker faker = new Faker();
        try {
            while (true) {
                producer.send(new ProducerRecord<>(
                        topic,
                        faker.dune().character(),
                        faker.dune().quote())).get();
                Thread.sleep(2000);
            }
        } catch (ExecutionException e) {
            logger.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }

    }
}
