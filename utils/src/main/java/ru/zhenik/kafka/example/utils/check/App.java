package ru.zhenik.kafka.example.utils.check;

public class App {
  public static void main(String[] args) {
    final StreamTestBehaviourProcessor
        streamTestBehaviourProcessor = new StreamTestBehaviourProcessor();
    streamTestBehaviourProcessor.run();
    Runtime.getRuntime().addShutdownHook(new Thread(streamTestBehaviourProcessor::stopStreams));
  }
}
