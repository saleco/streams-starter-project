package com.github.saleco;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class BankBalanceStream {

  /*

  kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bank-transactions

  kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bank-balance-exactly-once --config cleanup.policy=compact

  kafka-console-consumer.bat --bootstrap-server localhost:9092  --topic bank-balance-exactly-once --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

   */
  public static void main(String[] args) throws IOException {
    Properties properties = new Properties();

    //create consumer configs
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    //we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in production
    properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

    //JSON Serde
    final Serializer<JsonNode> jsonNodeSerializer = new JsonSerializer();
    final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
    final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonNodeSerializer, jsonDeserializer);


    KStreamBuilder builder = new KStreamBuilder();

    //stream data
    KStream<String, JsonNode> bankTransactions = builder.stream(Serdes.String(), jsonSerde, "bank-transactions");

    ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
    initialBalance.put("count", 0);
    initialBalance.put("balance", 0);
    initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

    KTable<String, JsonNode> bankBalance = bankTransactions
        .groupByKey(Serdes.String(), jsonSerde)
        .aggregate(() -> initialBalance,
            (key, transaction, balance) -> newBalance(transaction, balance),
            jsonSerde,
            "bank-ballance-agg");

    bankBalance.to(Serdes.String(), jsonSerde, "bank-balance-exactly-once");

    KafkaStreams streams = new KafkaStreams(builder, properties);
    streams.cleanUp();
    streams.start();

    System.out.println(streams.toString());

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
    ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
    newBalance.put("count", balance.get("count").asInt() + 1);
    newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());

    Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
    Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();

    Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
    newBalance.put("time", newBalanceInstant.toString());

    return newBalance;
  }


}
