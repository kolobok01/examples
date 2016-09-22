package io.confluent.examples.streams;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

import java.util.Properties;

/**
 * Created by kolobok on 9/21/16.
 */
public class ABExperimentEventDriver {

    public static void main(String[] args) throws IOException {
       produceInputs(100000);
    }


    private static void produceInputs(int num) throws IOException {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        final KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);
        final GenericRecordBuilder screenEvent =
                new GenericRecordBuilder(loadSchema("screenevent.avsc"));

        final String ScreenEvents = "ScreenEvents2";
        Random rnd = new Random();

        IntStream.range(0, num)
                .mapToObj(value -> {
                    String experiment = String.valueOf(rnd.nextInt(20));
                    String variant = getVariant();
                    Boolean converted = getRandomBoolean();
                    screenEvent.set("experiment_id", experiment);
                    screenEvent.set("variant", variant);
                    screenEvent.set("converted", converted);
                    return screenEvent.build();
                }).forEach(
                record -> producer.send(new ProducerRecord<>(ScreenEvents, String.valueOf(record.get("experiment_id")), record))
        );
        producer.flush();
    }

    private static String getVariant() {
        if(Math.random() < 0.5) return "a";
        else return "b";
    }

    private static boolean getRandomBoolean() {
        return Math.random() < 0.2;
    }

    private static Schema loadSchema(final String name) throws IOException {
        try (InputStream input = PageViewRegionLambdaExample.class.getClassLoader()
                .getResourceAsStream("avro/io/confluent/examples/streams/" + name)) {
            return new Schema.Parser().parse(input);
        }
    }
}
