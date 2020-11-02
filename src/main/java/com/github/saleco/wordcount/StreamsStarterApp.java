package com.github.saleco.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

  public Topology createTopology() {

    StreamsBuilder builder = new StreamsBuilder();

    //stream from kafka
    KStream<String, String> wordCountInput= builder.stream("word-count-input");

    // map values to lower case
    //flatmap values split by space
    //select key to apply a key (we discard the old key and put the value as a key)
    //group by key before aggregation
    //count occurrences

    KTable<String, Long> wordCounts =
        wordCountInput.mapValues(textLine -> textLine.toLowerCase())
            .flatMapValues(lowerCasedTextLine -> Arrays.asList(lowerCasedTextLine.split(" ")))
            .selectKey((ignoredKey, word) -> word)
            .groupByKey()
            .count(Materialized.as("Counts"));

    //to in order to write the results back to kafka
    wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

    return builder.build();

  }

  public static void main(String[] args) {
    Properties properties = new Properties();

    //create consumer configs
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    StreamsStarterApp streamsStarterApp = new StreamsStarterApp();

    KafkaStreams streams = new KafkaStreams(streamsStarterApp.createTopology(), properties);

    //only do this in dev
    streams.cleanUp();

    streams.start();

    System.out.println(streams.toString());

    //closes the stream application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
