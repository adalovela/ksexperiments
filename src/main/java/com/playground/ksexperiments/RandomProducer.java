package com.playground.ksexperiments;

import com.github.javafaker.Faker;
import com.playground.ksexperiments.utils.TopicsManager;
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

@RequiredArgsConstructor
public class RandomProducer {

    private static final Logger logger = LoggerFactory.getLogger(RandomProducer.class);

    private static String inputTopic;

    private static final int KEY_RANDOM_FACTOR = 3;

    public static final String FILTER_KEY_PREFIX = "TO_FILTER_";
    public static final String BRANCH_1_PREFIX = "BRANCH_1: ";
    public static final String BRANCH_2_PREFIX = "BRANCH_2: ";

    private static long messagesCounter;

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
                String  key = prefixKey(faker.dune().character());
                String  value =  decorateValue(faker.dune().quote());
                producer.send(new ProducerRecord<>(
                        topic,
                        key,
                       value)
                );
                logger.info(String.format("Sent -> Key:  %s - Value: %s", key, value));
                Thread.sleep(2000);
            }
        }  catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }

    }

    private static String decorateValue(String value) {
        messagesCounter++;
        if (messagesCounter % KEY_RANDOM_FACTOR == 0) {
            return BRANCH_1_PREFIX + value;
        } else {
            return BRANCH_2_PREFIX + value;
        }
    }

    private static String prefixKey(String key) {
        messagesCounter++;
        if (messagesCounter % KEY_RANDOM_FACTOR == 0) {
            return FILTER_KEY_PREFIX + key;
        } else {
            return key;
        }
    }
}
