package com.zenika;

import com.zenika.avro.price.Price;
import com.zenika.avro.price.PriceKey;
import com.zenika.avro.product.Product;
import com.zenika.avro.product.ProductKey;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

public class MainBranch {

    public static void main(String[] args) {

        final Properties streamsConfiguration = configureKafkaClient();

        final StreamsBuilder builder = new StreamsBuilder();

        // price stream
        final KStream<PriceKey, Price> prices = builder.stream("price_in_avro");
        prices.print(Printed.toSysOut());

        // Branches
        final KStream<PriceKey, Price>[] branches = prices.branch((priceKey, priceValue) -> priceValue.getPrice() < 100_000
                                                                , (priceKey, priceValue) -> priceValue.getPrice() > 900_000);

        branches[0].through("under-100_000");
        branches[1].through("over-900_000");


        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties configureKafkaClient() {
        final Properties streamsConfiguration = new Properties();


        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "http://127.0.0.1:9092");
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "branchesStreams");
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        return streamsConfiguration;
    }

}
