package com.playground.ksexperiments.topologies;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.playground.ksexperiments.RandomProducer.BRANCH_1_PREFIX;
import static com.playground.ksexperiments.RandomProducer.BRANCH_2_PREFIX;

public class BranchByValueTopology {

    private static final Logger logger = LoggerFactory.getLogger(BranchByValueTopology.class);

    private BranchByValueTopology(){}

    public static Topology create(String inputTopic, String branch1Topic, String branch2Topic) {
        logger.info(String.format("Proceeding to create a branch topology with:%n InputTopic: %s%nBranch1Topic: %s%nBranch2Topic: %s", inputTopic, branch1Topic, branch2Topic));
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();

        builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
                .peek((k,v) -> logger.info("Observed event to be branched- k: {} - v: {}", k, v))
                .split()
                .branch(
                        (key, value) -> value.startsWith(BRANCH_1_PREFIX),
                        Branched.withConsumer(ks -> ks.to(branch1Topic)))
                .branch((key, value) -> value.startsWith(BRANCH_2_PREFIX),
                        Branched.withConsumer(ks -> ks.to(branch2Topic)));

        return builder.build();
    }

}