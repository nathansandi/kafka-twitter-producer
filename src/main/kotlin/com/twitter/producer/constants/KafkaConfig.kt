package com.twitter.producer.constants


const val BOOTSTRAPSERVERS = "127.0.0.1:9092"
const val TOPIC = "Twitter-Kafka"
const val ACKS_CONFIG = "all"
const val MAX_IN_FLIGHT_CONN = "5"
const val COMPRESSION_TYPE = "snappy"
val RETRIES_CONFIG = Integer.toString(Int.MAX_VALUE)
const val LINGER_CONFIG = "20"
val BATCH_SIZE = Integer.toString(32 * 1024)
const val AVRO_SERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroSerializer"
const val SCHEMA_REGISTRY_SERVER_URL = "http://localhost:8081"