package com.playground.ksexperiments;

import com.playground.ksexperiments.topologies.FilterByKeyTopology;
import com.playground.ksexperiments.topologies.BranchByValueTopology;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.playground.ksexperiments.utils.TopicsManager.*;

@RequiredArgsConstructor
public class KStreamTopologyBootstrapper implements AutoCloseable {

    private final Properties props;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    private KafkaStreams filterStream;
    private KafkaStreams branchStream;

    public boolean start() {
        if(isStarted.compareAndSet(false, true)) {
            String appId = props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
            props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, appId + "_filter");
            filterStream = new KafkaStreams(filterTopology(), props);
            filterStream.start();

            props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, appId+ "_branching");
            branchStream = new KafkaStreams(branchTopology(), props);
            branchStream.start();
        }

        return isStarted.get();
    }

    private Topology filterTopology() {
        String inputTopic = props.getProperty(INITIAL_INPUT_TOPIC);
        String outputTopic = props.getProperty(FILTER_OUTPUT_TOPIC);

        return FilterByKeyTopology.create(inputTopic, outputTopic);
    }

    private Topology branchTopology() {
        String inputTopic = props.getProperty(FILTER_OUTPUT_TOPIC);
        String branch1Topic = props.getProperty(BRANCH1_OUTPUT_TOPIC);
        String branch2Topic = props.getProperty(BRANCH2_OUTPUT_TOPIC);

        return BranchByValueTopology.create(inputTopic, branch1Topic, branch2Topic);
    }

    @Override
    public void close()  {
        if(isStarted.compareAndSet(true, false)) {
            branchStream.close();
            filterStream.close();
        }
    }
}
