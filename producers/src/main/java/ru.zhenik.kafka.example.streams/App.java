package ru.zhenik.kafka.example.streams;

import ru.zhenik.kafka.example.utils.Util;

public class App {
  public static void main(String[] args) throws InterruptedException {
    final TimeoutProducers timeoutProducers = new TimeoutProducers(Util.TOPIC_REQUEST, Util.TOPIC_CONFIRMATION);
    timeoutProducers.sendRequestAndConfirmation(1000L);
  }
}
