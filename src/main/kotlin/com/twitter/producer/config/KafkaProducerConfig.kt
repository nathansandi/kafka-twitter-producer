package com.twitter.producer.config

import com.twitter.producer.constants.*
import com.twitter.producer.kafka.KafkaAvroMessageProducer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.stereotype.Component
import java.util.*

@Component
class KafkaProducerConfig {
    companion object {
        fun createKafkaProducer(): KafkaProducer<String, String> {
            // Create producer properties
            val prop = Properties()
            prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAPSERVERS)
            prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.getName())
            prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.getName())

            // create safe Producer
            prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
            prop.setProperty(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG)
            prop.setProperty(ProducerConfig.RETRIES_CONFIG, RETRIES_CONFIG)
            prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT_CONN)

            // Additional settings for high throughput producer
            prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE)
            prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, LINGER_CONFIG)
            prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE)

            // Setting Kafka AVRO
            prop.setProperty("key.serializer", AVRO_SERIALIZER_CLASS)
            prop.setProperty("value.serializer", AVRO_SERIALIZER_CLASS)
            prop.setProperty("schema.registry.url", SCHEMA_REGISTRY_SERVER_URL)

            // Create producer
            return KafkaProducer<String, String>(prop)
        }
    }
}