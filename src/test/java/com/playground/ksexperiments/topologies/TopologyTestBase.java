package com.playground.ksexperiments.topologies;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class TopologyTestBase {

    final static String TEST_CONFIG_FILE = "test.properties";

    final static Serializer<String> keySerializer = Serdes.String().serializer();
    final static Serializer<String> valueSerializer = Serdes.String().serializer();

    final static Deserializer<String> keyDeserializer = Serdes.String().deserializer();
    final static Deserializer<String> valueDeserializer = Serdes.String().deserializer();


}
