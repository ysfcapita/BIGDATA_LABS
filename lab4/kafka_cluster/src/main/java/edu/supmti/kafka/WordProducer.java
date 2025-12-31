package edu.supmti.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

public class WordProducer {
  private static final String TOPIC = "WordCount-Topic";

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093,localhost:9094");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
         BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {

      System.out.println("Tapez des mots (Ctrl+C pour quitter):");
      String line;
      while ((line = br.readLine()) != null) {
        String[] words = line.trim().split("\\s+");
        for (String w : words) {
          if (w.isEmpty()) continue;
          producer.send(new ProducerRecord<>(TOPIC, w.toLowerCase()), (metadata, exception) -> {
            if (exception != null) {
              System.err.println("Erreur envoi: " + exception.getMessage());
            } else {
              System.out.println("OK: " + w + " -> partition " + metadata.partition());
            }
          });
        }
        producer.flush();
      }
    }
  }
}
