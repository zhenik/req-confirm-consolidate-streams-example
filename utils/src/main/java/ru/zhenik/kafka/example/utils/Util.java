package ru.zhenik.kafka.example.utils;

import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

public class Util {

  public final static String prefix = "nt-";
  public final static String TOPIC_REQUEST = prefix+"requests-v1";
  public final static String TOPIC_CONFIRMATION = prefix+"confirmations-v1";
  public final static String TOPIC_CONSOLIDATION = prefix+"consolidations-v1";
  public final static String TOPIC_STATUS = prefix+"status-v1";

  public final static String REQUEST_PENDING = "request:PENDING";
  public final static String REQUEST_CONFIRMATION = "request:REQUEST_CONFIRMATION";
  public final static String REQUEST_CONSOLIDATED = "request:REQUEST_CONSOLIDATED";
  public final static String STATUS_CONSOLIDATED = "status:CONSOLIDATED";
  public final static String STATUS_NOT_CONSOLIDATED_YET = "status:NOT_CONSOLIDATED_YET";

  final AdminClient adminClient;

  private Util() {
    this.adminClient = AdminClient.create(getDefaultPropsAdmin());
  }

  public static Util instance() {
    return new Util();
  }

  private Properties getDefaultPropsAdmin() {
    final Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "admin-client-id-" + Instant.now().getEpochSecond());
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 500);
    return properties;
  }

  public void createTopics() {
    createTopic(TOPIC_REQUEST);
    createTopic(TOPIC_CONFIRMATION);
    createTopic(TOPIC_CONSOLIDATION);
    createTopic(TOPIC_STATUS);
  }

  public void createTopic(final String topic) {
    try {
      // Define topic
      final NewTopic newTopic = new NewTopic(topic, 1, (short) 1);

      // Create topic, which is async call.
      final CreateTopicsResult createTopicsResult =
          adminClient.createTopics(Collections.singleton(newTopic));

      // Since the call is Async, Lets wait for it to complete.
      createTopicsResult.values().get(topic).get();
    } catch (InterruptedException | ExecutionException e) {

      if (!(e.getCause() instanceof TopicExistsException)) {
        throw new RuntimeException(e.getMessage(), e);
      }

      // TopicExistsException - Swallow this exception, just means the topic already exists.
      System.out.println("Topic : " + topic + " already exists");
    }
  }
}
