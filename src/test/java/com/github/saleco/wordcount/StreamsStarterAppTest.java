package com.github.saleco.wordcount;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class StreamsStarterAppTest {

  TopologyTestDriver testDriver;

  StringSerializer stringSerializer = new StringSerializer();

  private ConsumerRecordFactory<String, String> recordFactory
      = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

  @Before
  public void setupTopology() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    StreamsStarterApp streamsStarterApp = new StreamsStarterApp();
    Topology topology = streamsStarterApp.createTopology();
    testDriver = new TopologyTestDriver(topology, properties);
  }

  @After
  public void closesTestDriver() {
    testDriver.close();
  }

  public void pushNewInputRecord(String value) {
    testDriver.pipeInput(recordFactory.create("word-count-input", null, value));
  }

  public ProducerRecord<String, Long> readOutput() {
    return testDriver.readOutput("word-count-output", new StringDeserializer(), new LongDeserializer());
  }

  @Test
  public void makeSureCountsAreCorrect() {
    String firstExample = "testing Kafka Streams";
    pushNewInputRecord(firstExample);
    OutputVerifier.compareKeyValue(readOutput(), "testing", 1L);
    OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
    OutputVerifier.compareKeyValue(readOutput(), "streams", 1L);

    assertEquals(readOutput(), null);

    String secondExample = "testing Kafka again";
    pushNewInputRecord(secondExample);
    OutputVerifier.compareKeyValue(readOutput(), "testing", 2L);
    OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
    OutputVerifier.compareKeyValue(readOutput(), "again", 1L);
  }

  @Test
  public void makeSureWordsBecomeLowerCase() {
    String upperCaseString = "KAFKA kafka Kafka";
    pushNewInputRecord(upperCaseString);
    OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
    OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
    OutputVerifier.compareKeyValue(readOutput(), "kafka", 3L);
  }
}
