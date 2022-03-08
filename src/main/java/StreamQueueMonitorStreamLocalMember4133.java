///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS com.rabbitmq:amqp-client:5.14.2
//DEPS org.slf4j:slf4j-simple:1.7.36

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamQueueMonitorStreamLocalMember4133 {

  public static void main(String[] args) throws Exception {
    ConnectionFactory cf = new ConnectionFactory();
    cf.setAutomaticRecoveryEnabled(false);
    cf.setTopologyRecoveryEnabled(false);

    try (Connection c1 = cf.newConnection();
        Connection c2 = cf.newConnection()) {
      Channel ch1 = c1.createChannel();
      ch1.queueDeclare(
          "sq", true, false, false, Collections.singletonMap("x-queue-type", "stream"));

      AtomicBoolean keepPublishing = new AtomicBoolean(true);
      new Thread(
              () -> {
                try {
                  while (keepPublishing.get()) {
                    ch1.basicPublish("", "sq", null, "hello".getBytes(StandardCharsets.UTF_8));
                    Thread.sleep(1000);
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                }
              })
          .start();

      Channel ch2 = c2.createChannel();
      ch2.basicQos(10);
      ch2.basicConsume(
          "sq",
          false,
          Collections.emptyMap(),
          (consumerTag, message) -> {
            long offset = (long) message.getProperties().getHeaders().get("x-stream-offset");
            System.out.println("Received message (offset " + offset + ")...");
            ch2.basicAck(message.getEnvelope().getDeliveryTag(), false);
          },
          consumerTag -> { });

      new Scanner(System.in).nextLine();
      keepPublishing.set(false);
    }
  }
}
