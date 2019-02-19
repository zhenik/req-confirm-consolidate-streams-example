package ru.zhenik.kafka.example.streams;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.utils.Bytes;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import static ru.zhenik.kafka.example.streams.StreamJoinApplication.topicStatus;

public class QueryTopicRest implements Runnable {
  private final KafkaStreams kafkaStreams;
  private final String storageName="status-storage";
  private final Util utils;
  private final QueryableStoreType<ReadOnlyKeyValueStore<String, String>> storageType;

  public QueryTopicRest() {
    this.utils = Util.instance();
    try {
      this.storageType = QueryableStoreTypes.keyValueStore();
      this.kafkaStreams = new KafkaStreams(buildTopology(), getDefault());
    } catch (Exception e) {
      throw new IllegalArgumentException("Error creating Kafka Streams topology");
    }

  }

  private Topology buildTopology() {
    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder.table(
        topicStatus,
        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(storageName)
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.String()));

    return streamsBuilder.build();
  }


  public String getValue(final String key) {
    String value = null;
    final ReadOnlyKeyValueStore<String, String> store;
    try {
      store = waitUntilStoreIsQueryable(storageName, storageType, kafkaStreams);
      value = store.get(key);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return value;
  }

  public List<String> getValues() {
    List<String> values = new ArrayList<>();
    try {
      final ReadOnlyKeyValueStore<String, String> store =
          waitUntilStoreIsQueryable(storageName, storageType, kafkaStreams);
      final KeyValueIterator<String, String> patternIterator = store.all();
      while (patternIterator.hasNext()) {
        String patternJson = patternIterator.next().value;
        values.add(patternJson);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return values;
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

  private static <T> T waitUntilStoreIsQueryable(
      final String storeName,
      final QueryableStoreType<T> queryableStoreType,
      final KafkaStreams streams)
      throws InterruptedException {
    while (true) {
      try {
        return streams.store(storeName, queryableStoreType);
      } catch (InvalidStateStoreException e) {
        e.printStackTrace();
        // store not yet ready for querying
        Thread.sleep(1000);
      }
    }
  }

  @Override public void run() {
    utils.createTopic(topicStatus);
    kafkaStreams.start();
  }

  void stopStreams() { Optional.ofNullable(kafkaStreams).ifPresent(KafkaStreams::close); }

  public static void main(String[] args) throws InterruptedException {
    final QueryTopicRest queryTopicRest = new QueryTopicRest();
    queryTopicRest.run();
    Runtime.getRuntime().addShutdownHook(new Thread(queryTopicRest::stopStreams));
    //queryTopicRest.getValue("c97d9e8b-a0ba-48db-9a0a-6b261a139ca8");
    Thread.sleep(5000L);
    final List<String> values = queryTopicRest.getValues();
    System.out.println(values);
  }





}
