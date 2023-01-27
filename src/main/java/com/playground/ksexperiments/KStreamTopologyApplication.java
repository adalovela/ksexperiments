package com.playground.ksexperiments;

import com.playground.ksexperiments.utils.TopicsManager;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.playground.ksexperiments.utils.TopicsManager.*;

public class KStreamTopologyApplication {

    private static KStreamTopologyBootstrapper bootstrapper;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("One argument expected but " + args.length + " received");
        }

        Properties props = new Properties();

        try (InputStream inputStream = Files.newInputStream(Paths.get(args[0]))) {
            props.load(inputStream);

            KStreamTopologyApplication app = new KStreamTopologyApplication(props);
            app.createTopics(props);

            app.start();
        }

    }

    private KStreamTopologyApplication(Properties props) {
        bootstrapper = new KStreamTopologyBootstrapper(props);
        Runtime.getRuntime().addShutdownHook(new Thread(bootstrapper::close));
    }

    private boolean start() {
        if (isStarted.compareAndSet(false, true)) {
            bootstrapper.start();
        }

        return isStarted.get();
    }

    private Collection<String> createTopics(Properties props) {
        TopicsManager topicsManager = new TopicsManager(props);
        String inputTopic = props.getProperty(INITIAL_INPUT_TOPIC);
        String filteredTopic = props.getProperty(FILTER_OUTPUT_TOPIC);
        String branch1Topic = props.getProperty(BRANCH1_OUTPUT_TOPIC);
        String branch2Topic = props.getProperty(BRANCH2_OUTPUT_TOPIC);

        List<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(new NewTopic(inputTopic, 1, (short) 3));
        newTopics.add(new NewTopic(filteredTopic, 1, (short) 3));
        newTopics.add(new NewTopic(branch1Topic, 1, (short) 3));
        newTopics.add(new NewTopic(branch2Topic, 1, (short) 3));

        return topicsManager.createTopics(newTopics)
                .getOrElseThrow(e -> new IllegalStateException("An error occurred while creating topics: ", e));
    }

}
