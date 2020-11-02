package com.github.saleco.user.event.enricher;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class UserEventEnricherApp {

  public static void main(String[] args) {
    Properties properties = new Properties();

    //create consumer configs
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "user-event-enricher-app");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();

    //we get a global table out of kafka. This table will be replicated on each kafka streams application
    //the key of our globalKTable is the user ID
    GlobalKTable<String, String> usersGlobalTable = builder.globalTable("user-table");

    //we get a stream of user purchases
    KStream<String, String> userPurchases = builder.stream("user-purchases");

    //we want to enrich that stream
    KStream<String, String> userPurchasesEnrichedJoin =
        userPurchases.join(usersGlobalTable,
            (key, value) -> key, //map from the (key,value) of this stream to the key of the GlobalKTable (join == clause)
            (userPurchase, userInfo) -> "Purchase=" +userPurchase + ",UserInfo=[" + userInfo + "]"
       );

    userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");

    //we want to enrich that stream with left join
    KStream<String, String> userPurchasesEnrichedLeftJoin =
      userPurchases.leftJoin(usersGlobalTable,
          (key, value) -> key,
          (userPurchase, userInfo) -> {
            if(userInfo != null) {
              return "Purchase=" +userPurchase + ",UserInfo=[" + userInfo + "]";
            } else {
              return "Purchase=" +userPurchase + ",UserInfo=null";
            }
          }
      );

    userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");

    KafkaStreams streams = new KafkaStreams(builder.build(), properties);
    streams.cleanUp();
    streams.start();

    System.out.println(streams.toString());

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

  }
}
