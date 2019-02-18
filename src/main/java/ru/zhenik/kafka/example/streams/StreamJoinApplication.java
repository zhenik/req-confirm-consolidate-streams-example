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

public class StreamJoinApplication implements Runnable {
  private final KafkaStreams kafkaStreams;
  private final Util utils;
  public final static String topicRequest = "requests-v1";
  public final static String topicConfirmation = "confirmations-v1";
  private final String topicConsolidation = "consolidations-v1";
  private final String topicStatus = "status-v1";

  public StreamJoinApplication() {
    this.kafkaStreams = new KafkaStreams(buildTopology(), getDefault());
    this.utils = Util.instance();
  }

  private Topology buildTopology() {
    final StreamsBuilder streamsBuilder = new StreamsBuilder();

    final KStream<String, String> requestsStream =
        streamsBuilder.stream(topicRequest, Consumed.with(Serdes.String(), Serdes.String()));

    final KStream<String, String> confirmationsStream =
        streamsBuilder.stream(topicConfirmation, Consumed.with(Serdes.String(), Serdes.String()));

    final KStream<String, String> consolidatedStream = requestsStream.join(
        // other topic to join
        confirmationsStream,
        // left value, right value -> return value
        (requestValue, confirmedValue) -> confirmedValue + " : consolidated",
        // window
        JoinWindows.of(Duration.ofSeconds(5)),
        // how to join (ser and desers)
        Joined.with(
            Serdes.String(), /* key */
            Serdes.String(), /* left value */
            Serdes.String()  /* right value */
        )
    );

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
        .to(topicConsolidation, Produced.with(Serdes.String(), Serdes.String()));

    statusStream
        .peek((k, v) -> System.out.println("Failed : "+k+" : "+v))
        .to(topicStatus, Produced.with(Serdes.String(), Serdes.String()));

    streamsBuilder
        .table(topicStatus, Consumed.with(Serdes.String(), Serdes.String()))
        .filter((k,v)->"request not confirmed yet".equalsIgnoreCase(v))
        .toStream()
        .peek((k,v)-> System.out.println(k+" : "+v));

    return streamsBuilder.build();
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
    utils.createTopic(topicRequest);
    utils.createTopic(topicConfirmation);
    utils.createTopic(topicConsolidation);
    utils.createTopic(topicStatus);
    kafkaStreams.start();
  }

  void stopStreams() { Optional.ofNullable(kafkaStreams).ifPresent(KafkaStreams::close); }

  public static void main(String[] args) {
    final StreamJoinApplication streamJoinApplication = new StreamJoinApplication();
    streamJoinApplication.run();
    Runtime.getRuntime().addShutdownHook(new Thread(streamJoinApplication::stopStreams));
  }
}
