package com.playground.ksexperiments.topologies;

import com.playground.ksexperiments.FilterByKeyTopology;
import com.playground.ksexperiments.Utils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class FilterByKeyTopologyTest {

    private final static String TEST_CONFIG_FILE = "test.properties";

    private TopologyTestDriver testDriver;

    private final Serializer keySerializer = Serdes.String().serializer();
    private final Serializer valueSerializer = Serdes.String().serializer();

    private final Deserializer keyDeserializer = Serdes.String().deserializer();
    private final Deserializer valueDeserializer = Serdes.String().deserializer();

    @Test
    public void testFilterStream() throws IOException {
        String fullPathTestProps = getClass().getClassLoader().getResource(TEST_CONFIG_FILE).getPath();
        Properties props = Utils.loadEnvProperties(fullPathTestProps);

        String inputTopic = props.getProperty("input.topic.name");
        String outputTopic = props.getProperty("filtered.topic.name");

        Topology topologyUUT = FilterByKeyTopology.createTopology(inputTopic, outputTopic);
        testDriver = new TopologyTestDriver(topologyUUT, props);

        String key1 = "aaa";
        String value1 = "Message 1";

        String key2 = "bbb";
        String value2 = "Message 2";

        String key3 = "mmm";
        String value3 = "Message 3";

        final TestInputTopic<String, String> filterTestInputTopic =
                testDriver.createInputTopic(inputTopic, keySerializer, valueSerializer);

        filterTestInputTopic.pipeInput(key1, value1);
        filterTestInputTopic.pipeInput(key2, value2);
        filterTestInputTopic.pipeInput(key3, value3);

        final TestOutputTopic<String, String> filterTestOutputTopic =
                testDriver.createOutputTopic(outputTopic, keyDeserializer, valueDeserializer);

        List<KeyValue<String, String>> expected = new ArrayList<>();
        expected.add(new KeyValue<>(key1, value1));
        expected.add(new KeyValue<>(key2, value2));
        List<KeyValue<String, String>> result = filterTestOutputTopic.readKeyValuesToList();

        assertEquals(expected, result);
    }

}