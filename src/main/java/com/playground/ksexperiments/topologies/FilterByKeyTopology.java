package com.playground.ksexperiments.topologies;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.playground.ksexperiments.RandomProducer.FILTER_KEY_PREFIX;

public class FilterByKeyTopology  {
    private static final Logger logger = LoggerFactory.getLogger(FilterByKeyTopology.class);

    private FilterByKeyTopology(){}

    public static Topology create(String inputTopic, String outputTopic) {
        logger.info(String.format("Proceeding to create a branch topology with:%n InputTopic: %s%nOutputTopic: %s", inputTopic, outputTopic));

        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();

        builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
                .peek((k,v) -> logger.info("Observed event to be filtered - k: {} - v: {}", k, v))
                .filter((key, value) -> keyPredicate(key))
                .to(outputTopic, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }

    private static boolean keyPredicate(String key) {
        return !key.startsWith(FILTER_KEY_PREFIX);
    }
}
