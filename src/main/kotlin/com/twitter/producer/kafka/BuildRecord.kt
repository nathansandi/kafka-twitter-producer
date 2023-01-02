package com.twitter.producer.kafka

import com.fasterxml.jackson.dataformat.avro.AvroMapper
import com.fasterxml.jackson.dataformat.avro.AvroSchema
import com.twitter.producer.constants.AVRO_SERIALIZER_CLASS
import com.twitter.producer.model.TweetCount
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*


object KafkaAvroMessageProducer {
    private const val TOPIC_NAME = "Twitter-Kafka"
    private const val KAFKA_SERVER_ADDRESS = "localhost:9092"
    private const val SCHEMA_REGISTRY_SERVER_URL = "http://localhost:8081"
    @Throws(com.fasterxml.jackson.databind.JsonMappingException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        // Kafka Producer Configurations
        val kafkaProps = Properties()
        kafkaProps["bootstrap.servers"] = KAFKA_SERVER_ADDRESS
        kafkaProps["key.serializer"] = AVRO_SERIALIZER_CLASS
        kafkaProps["value.serializer"] = AVRO_SERIALIZER_CLASS
        kafkaProps["schema.registry.url"] = SCHEMA_REGISTRY_SERVER_URL

        // Schema Generation For Our Customer Class
        val avroMapper = AvroMapper()
        val schema: AvroSchema = avroMapper.schemaFor(TweetCount::class.java)
        KafkaProducer<String, GenericRecord>(kafkaProps).use { producer ->

            // Publishing The Messages
            for (c in 0..99) {
                val customer: TweetCount = TweetCount("teste",c)
                val recordBuilder = GenericRecordBuilder(schema.getAvroSchema())
                recordBuilder.set("key_word", customer.key_word)
                recordBuilder.set("count",customer.count)
                val genericRecord: GenericRecord = recordBuilder.build()
                val producerRecord: ProducerRecord<String, GenericRecord> =
                    ProducerRecord<String, GenericRecord>(
                        TOPIC_NAME,
                        "tweet", genericRecord
                    )
                producer.send(producerRecord)
                println("Published message for customer: $customer")
            }
        }
    }
}