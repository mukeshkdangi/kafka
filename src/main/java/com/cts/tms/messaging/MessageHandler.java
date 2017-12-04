package com.cts.tms.messaging;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class MessageHandler {
    private static final String TOPIC             = "mukesh";
    private static final String BOOTSTRAP_SERVERS = "localhost:9091";
    private static final String CONSUMER_GID      = "config-consumer-group";

    public static Producer<String, String> createProducer() {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(prop);

    }

    public static Consumer<String, String> createConsumer() {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GID);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        Consumer<String, String> kafkaConsumer = new KafkaConsumer<>(prop);
        kafkaConsumer.subscribe(Arrays.asList(TOPIC));
        return kafkaConsumer;

    }

    public static void runProducer() {
        final Producer<String, String> producer = createProducer();
        try {

            for (long i = 0; i < 5; i++) {
                ProducerRecord<String, String> records = new ProducerRecord<String, String>(TOPIC,
                        "My Mom is best " + i);
                RecordMetadata output = producer.send(records).get();
                System.out.println("Sent record to Kafka Topic " + output.topic() + "Partition " + output.partition());
            }
        } catch (Exception e) {
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static void runConsumer() {

        final Consumer<String, String> consumer = createConsumer();
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                if (records.count() == 0) {
                    System.out.println("No record found");
                    break;
                }
                records.forEach(record -> {

                    System.out.println("KEY" + record.key());
                    System.out.println("Value" + record.value());
                });

                consumer.commitAsync();
            }

        } catch (Exception e) {

        } finally {
            consumer.close();
        }
    }

}
