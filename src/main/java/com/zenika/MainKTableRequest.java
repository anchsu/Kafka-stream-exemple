package com.zenika;

import com.zenika.avro.product.Product;
import com.zenika.avro.product.ProductKey;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Properties;

public class MainKTableRequest {

    public static void main(String[] args) {

        final Properties streamsConfiguration = configureKafkaClient();

        final StreamsBuilder builder = new StreamsBuilder();

        // product Table
        final KTable<ProductKey, Product> products = builder.table("product_in_avro", Materialized.as("product_table"));
        //products.toStream().print(Printed.toSysOut());

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        // Sleep 10s and request product with id 10.
        Runnable r = () -> {
            sleep(10);

            final ReadOnlyKeyValueStore<ProductKey, Product> productStore = streams.store("product_table", QueryableStoreTypes.<ProductKey, Product>keyValueStore());

            final Product product = productStore.get(ProductKey.newBuilder().setProductIdentifier("10").build());
            System.out.println("product: " + product);
        };
        r.run();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties configureKafkaClient() {
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "http://127.0.0.1:9092");
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "requestKTable");
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        return streamsConfiguration;
    }

    private static void sleep(int timeToSleep) {
        try {
            int i = 0;
            while (i < timeToSleep) {
                System.out.println("sleeping: " + i++);
                Thread.sleep(1_000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
