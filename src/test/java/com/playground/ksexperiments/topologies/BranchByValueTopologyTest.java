package com.playground.ksexperiments.topologies;

import com.playground.ksexperiments.utils.Utils;
import org.apache.kafka.streams.*;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.playground.ksexperiments.RandomProducer.BRANCH_1_PREFIX;
import static com.playground.ksexperiments.RandomProducer.BRANCH_2_PREFIX;
import static com.playground.ksexperiments.topologies.TopologyTestBase.*;
import static com.playground.ksexperiments.utils.TopicsManager.*;
import static org.junit.Assert.assertEquals;

public class BranchByValueTopologyTest {

    private TopologyTestDriver testDriver;

    @Test
    public void testBranchingStream() throws IOException {
        String fullPathTestProps = getClass().getClassLoader().getResource(TEST_CONFIG_FILE).getPath();
        Properties props = Utils.loadEnvProperties(fullPathTestProps);

        String inputTopic = props.getProperty(FILTER_OUTPUT_TOPIC);
        String branch1Topic = props.getProperty(BRANCH1_OUTPUT_TOPIC);
        String branch2Topic = props.getProperty(BRANCH2_OUTPUT_TOPIC);

        Topology topologyUUT = BranchByValueTopology.create(inputTopic, branch1Topic, branch2Topic);
        testDriver = new TopologyTestDriver(topologyUUT, props);

        String key1 = "aaa";
        String value1 = BRANCH_1_PREFIX + "Message 1";

        String key2 = "bbb";
        String value2 = BRANCH_2_PREFIX + "Message 2";

        String key3 = "ccc";
        String value3 = BRANCH_1_PREFIX + "Message 3";

        final TestInputTopic<String, String> filterTestInputTopic =
                testDriver.createInputTopic(inputTopic, keySerializer, valueSerializer);

        filterTestInputTopic.pipeInput(key1, value1);
        filterTestInputTopic.pipeInput(key2, value2);
        filterTestInputTopic.pipeInput(key3, value3);

        final TestOutputTopic<String, String> branchTestOutputTopic1 =
                testDriver.createOutputTopic(branch1Topic, keyDeserializer, valueDeserializer);
        List<KeyValue<String, String>> expected1 = new ArrayList<>();
        expected1.add(new KeyValue<>(key1, value1));
        expected1.add(new KeyValue<>(key3, value3));
        List<KeyValue<String, String>> result1 = branchTestOutputTopic1.readKeyValuesToList();

        final TestOutputTopic<String, String> branchTestOutputTopic2 =
                testDriver.createOutputTopic(branch2Topic, keyDeserializer, valueDeserializer);
        List<KeyValue<String, String>> expected2 = new ArrayList<>();
        expected2.add(new KeyValue<>(key2, value2));
        List<KeyValue<String, String>> result2 = branchTestOutputTopic2.readKeyValuesToList();

        assertEquals(expected1, result1);
        assertEquals(expected2, result2);
    }



}
