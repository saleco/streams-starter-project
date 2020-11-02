package com.github.saleco.favourite.colour;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class FavoriteColourApp {

  /*
    Create topic
      kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic favourite-colours-input
      kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic favourite-colours-output

    Create consumer
      kafka-console-consumer.bat --bootstrap-server localhost:9092  --topic favourite-colours-output
      --from-beginning
      --formatter kafka.tools.DefaultMessageFormatter
      --property print.key=true
      --property print.value=true
      --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
      --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

    Create producer
      kafka-console-producer.bat --broker-list localhost:9092 --topic favourite-colours-input

    Check the Results in console consumer

   */

  public static void main(String[] args) {

    Properties properties = new Properties();

    //create consumer configs
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-application");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    //we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in production
    properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

    KStreamBuilder builder = new KStreamBuilder();

    //stream data
    KStream<String, String> textLines = builder.stream("favourite-colours-input");

    //Filter bad data and Transform text stream into a key / value (user, colour)
    KStream<String, String> usersAndColours =
        textLines
            .filter((key, value) -> value.contains(","))
            .selectKey((key, value) -> value.split(",")[0].toLowerCase())
            .mapValues(value -> value.split(",")[1].toLowerCase())
            .filter((user, colour) -> Arrays.asList("green", "blue", "red").contains(colour));

    //sends data to intermediate output
    usersAndColours.to("favourite-colors-intermediate-output");

    //read the topic as a KTable so that updates are read correctly (remove duplicated values and keep the newer one)
    KTable<String, String> usersAndColoursTable = builder.table("favourite-colors-intermediate-output");

    //count the occurences of colors
    KTable<String, Long> favouriteColours = usersAndColoursTable
        .groupBy((user, color) -> new KeyValue<>(color, color))
        .count("CountsByColours");

    //output the results to the final output topic
    favouriteColours.to(Serdes.String(), Serdes.Long(), "favourite-colours-output");


    KafkaStreams streams = new KafkaStreams(builder, properties);

    streams.start();

    System.out.println(streams.toString());

    //closes the stream application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

}
