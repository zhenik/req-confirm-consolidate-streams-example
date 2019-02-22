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

/**
 * From doc about JoinWindow
 *    stream1.ts - before <= stream2.ts <= stream1.ts + after
 *
 * Options:
 * 1. before = after = time-difference
 * 2. before = 0                AND after = time-difference
 * 3. before = time-difference  AND after = 0
 *
 * When u do JoinWindows.of(Duration.ofSeconds(5)), is equal to
 * stream1.ts - 5 sec <= stream2.ts <= stream1.ts + 5 sec
 */
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

    final KStream<String, String> consolidatedStream = confirmationsStream.join(
        // other topic to join
        requestsStream,
        //confirmationsStream,
        // left value, right value -> return value
        (confirmedValue, requestValue) -> REQUEST_CONSOLIDATED,
        // window
        JoinWindows.of(Duration.ofSeconds(0)).before(Duration.ofSeconds(5)),
        // how to join (ser and desers)
        Joined.with(
            Serdes.String(), /* key */
            Serdes.String(), /* left value */
            Serdes.String()  /* right value */
        )
    );

    // how to define that
    final KStream<String, String> confirmationCameNeverStream = requestsStream.leftJoin(
        confirmationsStream,
        (requestValue, confirmationValue) ->
            String.format("requestValue: %s ,confirmationValue: %s", requestValue, confirmationValue),
        JoinWindows.of(Duration.ofSeconds(0)).after(Duration.ofSeconds(10)),
        Joined.with(
            Serdes.String(), /* key */
            Serdes.String(), /* left value */
            Serdes.String()  /* right value */
        )
    );

    // confirmations came late. Definition of time window should be >= (bigger or equal) with time window for consolidation
    final KStream<String, String> confirmationCameLateStream =
        confirmationsStream
            .leftJoin(
                consolidatedStream,
                (confirmationValue, consolidatedValue) -> {
                  System.out.println(String.format("confirmationValue: %s ,consolidatedValue: %s", confirmationValue, consolidatedValue));
                  return consolidatedValue==null ? "CAME_LATE" : consolidatedValue ;
                },
                // window
                JoinWindows.of(Duration.ofSeconds(0)).after(Duration.ofSeconds(5)),
                // how to join (ser and desers)
                Joined.with(
                    Serdes.String(), /* key */
                    Serdes.String(), /* left value */
                    Serdes.String() /* right value */))
            .filter( (ignored, value) -> "CAME_LATE".equalsIgnoreCase(value));

    confirmationCameNeverStream.to("nt-confirmation-came-never");
    confirmationCameLateStream.to("nt-confirmation-came-late");

    consolidatedStream
        .peek((k, v) -> System.out.println("Consolidated : "+k+" : "+v))
        .to(TOPIC_CONSOLIDATION, Produced.with(Serdes.String(), Serdes.String()));


    //final KStream<String, String> failureRequestStream =
    //    requestsStream.leftJoin(
    //        consolidatedStream,
    //        (requestValue, consolidatedValue) -> {
    //          System.out.printf("Before LeftJoin: [%s : %s]\n", requestValue, consolidatedValue);
    //          return String.format("requestValue: %s ,consolidatedValue: %s", requestValue, consolidatedValue);
    //        },
    //        // todo:  try with grace()
    //        // todo: try with session windows
    //        // https://kafka.apache.org/20/documentation/streams/developer-guide/dsl-api.html#session-windows
    //        JoinWindows.of(Duration.ofSeconds(5)).after(Duration.ofSeconds(5)),
    //        // how to join (ser and desers)
    //        Joined.with(
    //            Serdes.String(), /* key */
    //            Serdes.String(), /* left value */
    //            Serdes.String() /* right value */))
    //    .peek((k,v)-> System.out.printf("After LeftJoin: [%s : %s]\n", k, v));
    //
    //failureRequestStream
    //    .to(TOPIC_REQUEST_ERROR, Produced.with(Serdes.String(), Serdes.String()));


    //todo: consolidation-failed stream

    //final KStream<String, String> statusStream =
    //    requestsStream
    //        .outerJoin(
    //            // other topic to join
    //            consolidatedStream,
    //            // left value, right value -> return value
    //            (requestValue, confirmedValue) ->
    //                (confirmedValue == null) ? STATUS_NOT_CONSOLIDATED_YET : STATUS_CONSOLIDATED,
    //            // window
    //            JoinWindows.of(Duration.ofSeconds(5)),
    //            // how to join (ser and desers)
    //            Joined.with(
    //                Serdes.String(), /* key */
    //                Serdes.String(), /* left value */
    //                Serdes.String() /* right value */)
    //        );


    //statusStream
    //    .peek((k, v) -> System.out.println("Status : " +k+" : "+v))
    //    .to(TOPIC_STATUS, Produced.with(Serdes.String(), Serdes.String()));

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
