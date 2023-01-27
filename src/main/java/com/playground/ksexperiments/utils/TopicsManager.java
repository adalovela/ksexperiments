package com.playground.ksexperiments.utils;

import io.vavr.control.Try;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class TopicsManager {

    private final Logger logger = LoggerFactory.getLogger(TopicsManager.class);

    public static final String INITIAL_INPUT_TOPIC = "input.topic.name";
    public static final String FILTER_OUTPUT_TOPIC = "filtered.topic.name";
    public static final String BRANCH1_OUTPUT_TOPIC = "branch1.topic.name";
    public static final String BRANCH2_OUTPUT_TOPIC = "branch2.topic.name";
    public static final String MERGED_OUTPUT_TOPIC = "merged.topic.name";
    public static final String REKEYED_OUTPUT_TOPIC = "rekeyed.topic.name";

    private final Properties props;

    public Try<Collection<String>> createTopics(Collection<NewTopic> topics) {
        return Try.of(() -> {
            AdminClient client = AdminClient.create(props);
            Collection<String> names = extractTopicNames(createNewTopics(client, topics));
            return extractTopicInfo(client, names);
        });
    }

    private Collection<NewTopic> createNewTopics(AdminClient client, Collection<NewTopic> topics) {
        logger.info("Creating topics...");

        Collection<NewTopic> nonExistingTopics = topics
                .stream()
                .filter(topic -> {
                    try {
                        return !checkTopicExists(client, topic.name());
                    } catch (ExecutionException | InterruptedException e ) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());

        client.createTopics(nonExistingTopics)
                .values()
                .forEach( (topic, future) -> {
                    try {
                        future.get();
                        logger.info(String.format("Created topic %s", topic));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        return topics;
    }

    private Collection<String> extractTopicNames(Collection<NewTopic> topics) {
        return topics
                .stream()
                .map(NewTopic::name)
                .collect(Collectors.toCollection(LinkedList::new));
    }

    private Collection<String> extractTopicInfo(AdminClient client, Collection<String> topicNames) throws ExecutionException, InterruptedException, TimeoutException {
        return client.describeTopics(topicNames)
                .allTopicNames()
                .get(10, TimeUnit.SECONDS)
                .values()
                .stream()
                .map(topicDescription -> String.format("Topic Description: %s", topicDescription))
                .collect(Collectors.toList());
    }

    private boolean checkTopicExists(AdminClient client, String topicName) throws ExecutionException, InterruptedException {
        return client.listTopics().names().get().stream().anyMatch(name -> name.equalsIgnoreCase(topicName));
    }

}
