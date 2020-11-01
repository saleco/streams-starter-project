# Kafka Streams Project

## FavoriteColourApp instructions
Start zookeeper

Start Kafka

##Create topics

  `kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic favourite-colours-input`
  
  `kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic favourite-colors-intermediate-output`

  `kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic favourite-colours-output`

##Create consumer

  `kafka-console-consumer.bat --bootstrap-server localhost:9092  --topic favourite-colours-output
  --from-beginning
  --formatter kafka.tools.DefaultMessageFormatter
  --property print.key=true
  --property print.value=true
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer`


##Create producer

  `kafka-console-producer.bat --broker-list localhost:9092 --topic favourite-colours-input`

##Start typing some data

stephane,blue

john,green

stephane,red

alice,red
  
## Check the Results in console consumer
blue    1

green   1

red     1

blue    0

red     2
