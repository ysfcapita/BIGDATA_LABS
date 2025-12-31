package edu.supmti.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class WordCountConsumer {
  private static final String TOPIC = "WordCount-Topic";

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093,localhost:9094");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "wordcount-group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    Map<String, Integer> counts = new HashMap<>();

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(TOPIC)); // subscribe + poll loop [web:19]

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500)); // poll pattern [web:19]
        for (ConsumerRecord<String, String> r : records) {
          String word = r.value();
          counts.put(word, counts.getOrDefault(word, 0) + 1);
          System.out.println(word + " -> " + counts.get(word));
        }
      }
    }
  }
}
