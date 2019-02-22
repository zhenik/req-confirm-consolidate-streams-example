package ru.zhenik.kafka.example.utils.check;

import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import ru.zhenik.kafka.example.utils.Util;

import static ru.zhenik.kafka.example.utils.Util.TOPIC_REQUEST;

public class StreamTestBehaviourProcessor implements Runnable {
  private final KafkaStreams kafkaStreams;
  private final Util utils;

  StreamTestBehaviourProcessor() {
    this.kafkaStreams = new KafkaStreams(buildTopology(), getDefault());
    this.utils = Util.instance();
  }

  private Topology buildTopology() {
    final StreamsBuilder streamsBuilder = new StreamsBuilder();

    final KStream<String, String> requestsStream =
        streamsBuilder.stream(TOPIC_REQUEST, Consumed.with(Serdes.String(), Serdes.String()));

    requestsStream
        .mapValues(
            event -> {
              try {
                Thread.sleep(1000L);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              return event + " :step 1";
            })
        .peek((k, v) -> System.out.printf("KeyValue [%s : %s]\n", k, v))
        .through("topic-1")
        .mapValues(
            event -> {
              try {
                Thread.sleep(1000L);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              return event + " :step 2";
            })
        .peek((k, v) -> System.out.printf("KeyValue [%s : %s]\n", k, v))
        .to("topic-2");

    final Topology topology = streamsBuilder.build();
    System.out.println("Topology\n"+topology.describe());
    return topology;
  }


  private Properties getDefault() {
    final Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-app-id-" + Instant.now().getEpochSecond());
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, "stream-client-id-" + Instant.now().getEpochSecond());
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return properties;
  }

  @Override public void run() {
    utils.createTopics();
    kafkaStreams.start();
  }

  void stopStreams() { Optional.ofNullable(kafkaStreams).ifPresent(KafkaStreams::close); }


}
