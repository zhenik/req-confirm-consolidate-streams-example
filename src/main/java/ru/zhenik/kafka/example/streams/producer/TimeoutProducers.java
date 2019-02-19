package ru.zhenik.kafka.example.streams.producer;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.zhenik.kafka.example.streams.StreamJoinApplication;

public class TimeoutProducers {


  private final KafkaProducer<String, String> producer;
  private final String topicRequest;
  private final String topicConfirmation;
  public final static String REQUEST = "request";
  public final static String CONFIRMATION = "confirmation";

  public TimeoutProducers(final String topicRequest, final String topicConfirmation) {
    this.producer = new KafkaProducer<>(getDefault());
    this.topicRequest = topicRequest;
    this.topicConfirmation = topicConfirmation;
  }


  public void sendRequestAndConfirmation(final Long deltaTime) throws InterruptedException {
    final String key = UUID.randomUUID().toString();
    producer.send(new ProducerRecord<>(topicRequest, key, REQUEST));
    Thread.sleep(deltaTime);
    producer.send(new ProducerRecord<>(topicConfirmation, key, CONFIRMATION));
  }


  private Properties getDefault() {
    final Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-id-"+ Instant.now().toEpochMilli());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.RETRIES_CONFIG, 0);
    return properties;
  }

  public static void main(String[] args) throws InterruptedException {
    final TimeoutProducers timeoutProducers = new TimeoutProducers(StreamJoinApplication.topicRequest,StreamJoinApplication.topicConfirmation);
    timeoutProducers.sendRequestAndConfirmation(6000L);
  }
}
