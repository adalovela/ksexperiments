package com.playground.ksexperiments;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterByKeyTopology  {
    private static final Logger logger = LoggerFactory.getLogger(FilterByKeyTopology.class);

    private FilterByKeyTopology(){}

    public static Topology createTopology(String inputTopic, String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();

        builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
                .peek((k,v) -> logger.info("Observed event: {}", v))
                .filter((key, value) -> initialCharacterPredicate(key))
                .to(outputTopic, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }

    private static boolean initialCharacterPredicate(String characterName) {
        return characterName.substring(0, 1).toUpperCase().matches("^[A-I].*$");
    }
}
