package com.playground.ksexperiments;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.playground.ksexperiments.TopicsManager.*;

@RequiredArgsConstructor
public class KStreamTopologyBootstrapper implements AutoCloseable {

    private final Properties props;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    private KafkaStreams filterStream;

    public boolean start() {
        if(isStarted.compareAndSet(false, true)) {
            filterStream = new KafkaStreams(filterTopology(), props);
            filterStream.start();
        }

        return isStarted.get();
    }

    private Topology filterTopology() {
        String inputTopic = props.getProperty(FILTER_INPUT_TOPIC);
        String outputTopic = props.getProperty(FILTER_OUTPUT_TOPIC);

        return FilterByKeyTopology.createTopology(inputTopic, outputTopic);
    }

    @Override
    public void close()  {
        if(isStarted.compareAndSet(true, false)) {
            filterStream.close();
        }
    }
}
