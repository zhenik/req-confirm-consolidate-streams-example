package ru.zhenik.kafka.example.streams;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;
import java.util.concurrent.ExecutorService;

public class StatusStateQueryApplication extends Application<AppConfig> {

  public static void main(String[] args) throws Exception {
    new StatusStateQueryApplication().run("server", "config.yml");
  }

  @Override public void run(AppConfig configuration, Environment environment) throws Exception {
    // stream app for query
    final StatusStateStreamProcessor statusStateStreamProcessor = new StatusStateStreamProcessor();
    final ExecutorService executorService =
        environment.lifecycle()
            .executorService("status-state-processor")
            .maxThreads(1)
            .build();
    executorService.submit(statusStateStreamProcessor);

    // rest interface
    final StatusResources statusResources = new StatusResources(statusStateStreamProcessor);
    environment.jersey().register(statusResources);
  }
}
