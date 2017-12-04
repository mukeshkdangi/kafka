package com.cts.tms.messaging;

public class KafkaHandler {

    public static void main(String arg[]) {
        MessageHandler.runProducer();
        MessageHandler.runConsumer();
    }
}
