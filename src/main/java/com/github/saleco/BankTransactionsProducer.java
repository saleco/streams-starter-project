package com.github.saleco;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionsProducer {

  /*
    Create a bank balance topic
    Output ~100 messages per second to the topic
    Each message is random in money (a positive value) and outputs evenly transactions for 6 customers

   */

  public static void main(String[] args) throws IOException {
    String bootstrapServers = "127.0.0.1:9092";

    //create Producer properties
    Properties properties = new Properties();

    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");  //strongest producing garantee
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); //ensure we dont push duplicates


    //create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    int i = 0;
    String topic = "bank-transactions";

    while (true) {

      try {
        String value = createRandomTransaction(new Random().nextInt(6));
        String key = new ObjectMapper().readValue(value, Transaction.class).getClient();

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record);
        Thread.sleep(100);

        record = new ProducerRecord<>(topic, key, value);
        producer.send(record);
        Thread.sleep(100);

        record = new ProducerRecord<>(topic, key, value);
        producer.send(record);
        Thread.sleep(100);
        i++;

      } catch (InterruptedException e) {
        break;
      }

    }

    producer.close();
  }

  protected static String createRandomTransaction(Integer randomInt) throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    Transaction transaction = null;

    switch (randomInt) {
      case 0:
        transaction = createJohnTransaction();
        break;
      case 1:
        transaction = createMarieTransaction();
        break;
      case 2:
        transaction = createStephaneTransaction();
        break;
      case 3:
        transaction = createBrunoTransaction();
        break;
      case 4:
        transaction = createJoeTransaction();
        break;
      case 5:
        transaction = createClaireTransaction();
        break;
    }

    return mapper.writeValueAsString(transaction);
  }

  private static Transaction createTransaction(String client) {
    Transaction transaction = new Transaction();
    transaction.setClient(client);
    Random random = new Random();
    transaction.setAmount(ThreadLocalRandom.current().nextInt(0, 100));
    transaction.setTime(Instant.now().toString());
    return transaction;
  }

  private static Transaction createJohnTransaction() {
    return createTransaction("john");
  }

  private static Transaction createMarieTransaction() {
    return createTransaction("marie");
  }

  private static Transaction createStephaneTransaction() {
    return createTransaction("stephane");
  }

  private static Transaction createBrunoTransaction() {
    return createTransaction("bruno");
  }

  private static Transaction createJoeTransaction() {
    return createTransaction("joe");
  }

  private static Transaction createClaireTransaction() {
    return createTransaction("claire");
  }
}
