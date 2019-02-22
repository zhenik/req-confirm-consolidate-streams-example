package ru.zhenik.kafka.example.streams;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import ru.zhenik.kafka.example.utils.Util;

public class App {
  public static void main(String[] args) throws InterruptedException, ExecutionException {
    final TimeoutProducers timeoutProducers = new TimeoutProducers(Util.TOPIC_REQUEST, Util.TOPIC_CONFIRMATION);

    String idInTime = "CONFIRMATION-IN-TIME-A";
    String idNever = "CONFIRMATION-COME-NEVER-A";
    String idCameLate = "CONFIRMATION-COME-LATE-A";

    //timeoutProducers.sendRequestAndConfirmation(1000L, idInTime);
    //timeoutProducers.sendRequestOnly(idNever); //todo: came-never ???
    timeoutProducers.sendRequestAndConfirmation(8000L, idCameLate);

    //final List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5);
    //for (Integer i : integers){
    //  System.out.println(i);
    //  timeoutProducers.sendRequestAndConfirmation(10L);
    //}
  }
}
