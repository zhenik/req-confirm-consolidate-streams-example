package ru.zhenik.kafka.example.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
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
import ru.zhenik.kafka.example.utils.Util;

import static ru.zhenik.kafka.example.utils.Util.TOPIC_STATUS;

public class StatusStateStreamProcessor implements Runnable {
  private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final KafkaStreams kafkaStreams;
  private final String storageName="status-storage";
  private final Util util;
  private final QueryableStoreType<ReadOnlyKeyValueStore<String, String>> storageType;

  public StatusStateStreamProcessor() {
    this.util = Util.instance();
    this.storageType = QueryableStoreTypes.keyValueStore();
    this.kafkaStreams = new KafkaStreams(buildTopology(), getDefault());
  }

  private Topology buildTopology() {
    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder.table(
        TOPIC_STATUS,
        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(storageName)
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.String()));

    final Topology topology = streamsBuilder.build();
    System.out.println("Topology\n"+topology.describe());
    return topology;
  }


  public KeyValueRepresentation getValue(final String key) {
    KeyValueRepresentation representation = null;
    final ReadOnlyKeyValueStore<String, String> store;
    try {
      store = waitUntilStoreIsQueryable(storageName, storageType, kafkaStreams);
      String value = store.get(key);
      if (value!=null) representation=new KeyValueRepresentation(key, value);

    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return representation;
  }

  public List<KeyValueRepresentation> getValues() {
    List<KeyValueRepresentation> values = new ArrayList<>();
    try {
      final ReadOnlyKeyValueStore<String, String> store = waitUntilStoreIsQueryable(storageName, storageType, kafkaStreams);
      final KeyValueIterator<String, String> patternIterator = store.all();
      while (patternIterator.hasNext()) {
        final KeyValue<String, String> next = patternIterator.next();
        values.add(new KeyValueRepresentation(next.key, next.value));
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
    util.createTopics();
    kafkaStreams.start();
  }

  void stopStreams() { Optional.ofNullable(kafkaStreams).ifPresent(KafkaStreams::close); }

}
