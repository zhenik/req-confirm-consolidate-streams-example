package ru.zhenik.kafka.example.streams;


import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import ru.zhenik.kafka.example.utils.Util;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

class TimeoutProducers {

  private final KafkaProducer<String, String> producer;
  private final Util util;


  TimeoutProducers(final String topicRequest, final String topicConfirmation) {
    this.producer = new KafkaProducer<>(getDefault());
    this.util = Util.instance();
  }


  void sendRequestAndConfirmation(final Long deltaTime) throws InterruptedException {
    final String key = UUID.randomUUID().toString();
    producer.send(new ProducerRecord<>(Util.TOPIC_REQUEST, key, Util.REQUEST_PENDING));
    Thread.sleep(deltaTime);
    producer.send(new ProducerRecord<>(Util.TOPIC_CONFIRMATION, key, Util.REQUEST_CONFIRMATION));
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

}
