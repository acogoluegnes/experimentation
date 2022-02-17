///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS com.rabbitmq:amqp-client:5.14.2
//DEPS org.slf4j:slf4j-simple:1.7.36

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.util.Scanner;

public class ConsumeFromQuorumQueue {

  static final String QUEUE = "qq";

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      log("1 argument required, publishing sequence of unconfirmed message");
    }
    long targetPublishingSequence = Long.valueOf(args[0]);
    ConnectionFactory cf = new ConnectionFactory();

    try (Connection c = cf.newConnection()) {
      Channel ch = c.createChannel();

      ch.basicConsume(
          QUEUE,
          true,
          (consumerTag, message) -> {
            String body = new String(message.getBody());
            long publishingSequence = Long.valueOf(body.split(" ")[1]);
            if (publishingSequence == targetPublishingSequence) {
              log("Found message '%s'", body);
            }
          },
          consumerTag -> {});
      Scanner keyboard = new Scanner(System.in);
      keyboard.nextLine();
    }
  }

  static void log(String format, Object... args) {
    System.out.println(String.format(format, args));
  }
}
