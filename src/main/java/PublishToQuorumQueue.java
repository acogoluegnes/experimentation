///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS com.rabbitmq:amqp-client:5.14.2
//DEPS org.slf4j:slf4j-simple:1.7.36

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;

public class PublishToQuorumQueue {

  static final String QUEUE = "qq";
  static final int COUNT = 1_000_000;
  static final int MAX_OUTSTANDING_CONFIRMS = 3000;
  static final Semaphore CONFIRM_SEMAPHORE = new Semaphore(MAX_OUTSTANDING_CONFIRMS);
  static final ConcurrentNavigableMap<Long, Boolean> UNCONFIRMED = new ConcurrentSkipListMap<>();

  public static void main(String[] args) throws Exception {
    ConnectionFactory cf = new ConnectionFactory();

    try (Connection c = cf.newConnection()) {
      Channel ch = c.createChannel();
      ch.queueDeclare(
          QUEUE, true, false, false, Collections.singletonMap("x-queue-type", "quorum"));
      ch.queuePurge(QUEUE);

      ch.addConfirmListener(
          (deliveryTag, multiple) -> handleAckNack(deliveryTag, multiple),
          (deliveryTag, multiple) -> handleAckNack(deliveryTag, multiple));

      ch.confirmSelect();

      for (int i = 0; i < COUNT; i++) {
        CONFIRM_SEMAPHORE.acquire();
        long publishingSequence = ch.getNextPublishSeqNo();
        UNCONFIRMED.put(publishingSequence, Boolean.TRUE);
        ch.basicPublish("", QUEUE, null, ("hello " + i).getBytes(StandardCharsets.UTF_8));
        if ((i + 1) % 10_000 == 0) {
          log("Published %d messages", i + 1);
        }
      }

      waitForPublishConfirms();
    }
  }

  static void handleAckNack(long seqNo, boolean multiple) {
    int numConfirms;

    log(seqNo + " " + multiple);
    if (multiple) {
      ConcurrentNavigableMap<Long, Boolean> confirmed = UNCONFIRMED.headMap(seqNo, true);
      numConfirms = confirmed.size();
      confirmed.clear();
    } else {
      UNCONFIRMED.remove(seqNo);
      numConfirms = 1;
    }

    if (numConfirms > 0) {
      CONFIRM_SEMAPHORE.release(numConfirms);
    }
  }

  static void waitForPublishConfirms() {
    log("Publish confirms enabled, making sure all messages have been confirmed");
    log("Outstanding publish confirm(s): %d", UNCONFIRMED.size());
    long timeout = Duration.ofSeconds(10).toMillis();
    long waited = 0;
    long waitTime = 100;
    while (waited <= timeout) {
      if (UNCONFIRMED.isEmpty()) {
        log("All messages have been confirmed, moving on...");
        waited = timeout;
      }
      try {
        Thread.sleep(waitTime);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        waited = timeout;
      }
      waited += waitTime;
    }
    if (waited > timeout) {
      log("Unconfirmed message(s): %s", UNCONFIRMED);
    }
  }

  static void log(String format, Object... args) {
    System.out.println(String.format(format, args));
  }
}
