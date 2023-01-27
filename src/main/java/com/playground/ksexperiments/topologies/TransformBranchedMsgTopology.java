package com.playground.ksexperiments.topologies;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformBranchedMsgTopology {

    private static final Logger logger = LoggerFactory.getLogger(TransformBranchedMsgTopology.class);

    private TransformBranchedMsgTopology() {}

    public static Topology create(String inputTopic, String outputTopic) {
        logger.info(String.format("Proceeding to create a transform topology with:%n InputTopic: %s%nOutputTopic: %s", inputTopic, outputTopic));

        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();

        builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
                .peek((k,v) -> logger.info("Observed event to be transformed - k: {} - v: {}", k, v))
                .print()
                .flatMapValues( value -> keyPredicate(key))
                .to(outputTopic, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }

    private

}
