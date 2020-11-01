package com.github.saleco;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

  public static void main(String[] args) {
    Properties properties = new Properties();

    //create consumer configs
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    KStreamBuilder builder = new KStreamBuilder();

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
            .count("Counts");

    //to in order to write the results back to kafka
    wordCounts.to(Serdes.String(), Serdes.Long(), "word-count-output");

    KafkaStreams streams = new KafkaStreams(builder, properties);

    streams.start();

    System.out.println(streams.toString());

    //closes the stream application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
