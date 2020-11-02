package com.github.saleco.user.event.enricher;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class UserDataProducer {

  public static void main(String[] args) throws InterruptedException, ExecutionException {
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

    Scanner sc = new Scanner(System.in);

    //create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    //we are going to test different scenarios to illustrate the join

    //1 we create a new user, then we send some data to Kafka
    System.out.println("\nExample 1 - new user \n");
    producer.send(userRecord("john", "First=John,Last=Doe,Email=john.doe@gmail.com")).get(); //NEVER USER .get() TESTING PURPOSES ONLY
    producer.send(purchaseRecord("john", "Apples and Bananas (1")).get();

    Thread.sleep(10000);

    //2 we receive user purchase, but it doesnti exist in kafka
    System.out.println("\nExample 2 - non existing user \n");
    producer.send(purchaseRecord("bob", "Kafka Udemy Course (2)")).get(); //NEVER USER .get() TESTING PURPOSES ONLY

    Thread.sleep(10000);

    //3 we update user "john" and send a new transaction
    System.out.println("\nExample 3 - update to user \n");
    producer.send(userRecord("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com")).get(); //NEVER USER .get() TESTING PURPOSES ONLY
    producer.send(purchaseRecord("john", "Oranges (3)")).get(); //NEVER USER .get() TESTING PURPOSES ONLY

    Thread.sleep(10000);

    //4 - we send a user purchase for stephane, but it exists in Kafka later
    System.out.println("\nExample 4 - non existing user then user \n");
    producer.send(purchaseRecord("stephane", "Computer (4)")).get();
    producer.send(userRecord("stephane", "First=Stephane,Last=Maarek,GitHub=simplesteph")).get();
    producer.send(purchaseRecord("stephane", "Books (4)")).get();
    producer.send(userRecord("stephane", null)).get(); //delete for cleanup

    Thread.sleep(10000);

    //5 we create a user, but it gets deleted before any purchase comes though
    System.out.println("\nExample 5 - user then delete then data\n");
    producer.send(userRecord("alice", "First=Alice")).get();
    producer.send(userRecord("alice", null)).get(); //that the delete record
    producer.send(purchaseRecord("alice", "Apacha Kafka Series (5")).get();

    Thread.sleep(10000);

    System.out.println("End of demo");

    producer.close();

  }

  private static ProducerRecord<String, String> purchaseRecord(String key, String value) {
    return new ProducerRecord<>("user-purchases", key, value);
  }

  private static ProducerRecord<String, String> userRecord(String key, String value) {
    return new ProducerRecord<>("user-table", key, value);
  }

}
