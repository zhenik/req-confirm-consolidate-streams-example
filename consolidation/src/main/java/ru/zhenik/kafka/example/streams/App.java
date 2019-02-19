package ru.zhenik.kafka.example.streams;

public class App {
  public static void main(String[] args) {
    final StreamJoinApplication streamJoinApplication = new StreamJoinApplication();
    streamJoinApplication.run();
    Runtime.getRuntime().addShutdownHook(new Thread(streamJoinApplication::stopStreams));
  }
}
