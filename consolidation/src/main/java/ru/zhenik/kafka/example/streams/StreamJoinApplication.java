package ru.zhenik.kafka.example.streams;

import java.time.Duration;
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
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import ru.zhenik.kafka.example.utils.Util;

import static ru.zhenik.kafka.example.utils.Util.*;


public class StreamJoinApplication implements Runnable {
  private final KafkaStreams kafkaStreams;
  private final Util utils;

  StreamJoinApplication() {
    this.kafkaStreams = new KafkaStreams(buildTopology(), getDefault());
    this.utils = Util.instance();
  }

  private Topology buildTopology() {
    final StreamsBuilder streamsBuilder = new StreamsBuilder();

    final KStream<String, String> requestsStream =
        streamsBuilder.stream(TOPIC_REQUEST, Consumed.with(Serdes.String(), Serdes.String()));

    final KStream<String, String> confirmationsStream =
        streamsBuilder.stream(TOPIC_CONFIRMATION, Consumed.with(Serdes.String(), Serdes.String()));

    final KStream<String, String> consolidatedStream = requestsStream.join(
        // other topic to join
        confirmationsStream,
        // left value, right value -> return value
        (requestValue, confirmedValue) -> CONSOLIDATED,
        // window
        JoinWindows.of(Duration.ofSeconds(5)),
        // how to join (ser and desers)
        Joined.with(
            Serdes.String(), /* key */
            Serdes.String(), /* left value */
            Serdes.String()  /* right value */
        )
    );

    //todo: consolidation-failed stream

    final KStream<String, String> statusStream =
        requestsStream
            .outerJoin(
                // other topic to join
                consolidatedStream,
                // left value, right value -> return value
                (requestValue, confirmedValue) ->
                    (confirmedValue == null) ? requestValue + " not confirmed yet": requestValue + " confirmed",
                // window
                JoinWindows.of(Duration.ofSeconds(5)),
                // how to join (ser and desers)
                Joined.with(
                    Serdes.String(), /* key */
                    Serdes.String(), /* left value */
                    Serdes.String() /* right value */)
            );

    consolidatedStream
        .peek((k, v) -> System.out.println("Consolidated : "+k+" : "+v))
        .to(TOPIC_CONSOLIDATION, Produced.with(Serdes.String(), Serdes.String()));

    statusStream
        .peek((k, v) -> System.out.println("Status : " +k+" : "+v))
        .to(TOPIC_STATUS, Produced.with(Serdes.String(), Serdes.String()));

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
