package com.twitter.producer

import com.fasterxml.jackson.dataformat.avro.AvroMapper
import com.fasterxml.jackson.dataformat.avro.AvroSchema
import com.twitter.hbc.core.Client
import com.twitter.producer.config.KafkaProducerConfig
import com.twitter.producer.config.TwitterConfig
import com.twitter.producer.constants.TOPIC
import com.twitter.producer.model.TweetCount
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.collections.HashMap


//private val logger: Logger = LoggerFactory.getLogger(ProducerApplication::class.java)

var countPositiveLula = 0;
var countNegativeLula = 0;
var countNeutralLula = 0;

var countPositiveBolsonaro = 0;
var countNegativeBolsonaro = 0;
var countNeutralBolsonaro = 0;

var map =  HashMap<String, Pair<String,Int>>();


var positive = arrayOf("bom", "honesto", "feliz", "educacao", "povo", "bem")
var negative = arrayOf("corrupto", "ladr\\u00e3o", "preso", "corrup\\u00e7\\u00e3o", "crime", "criminoso")

private var client: Client? = null
private var producer: KafkaProducer<String, GenericRecord>? = null
private val msgQueue: BlockingQueue<String> = LinkedBlockingQueue(100)

val avroMapper = AvroMapper()
val schema: AvroSchema = avroMapper.schemaFor(TweetCount::class.java)

@SpringBootApplication
class ProducerApplication

fun main(args: Array<String>) {
	Timer().scheduleAtFixedRate( object : TimerTask() {
		override fun run() {
			sendDataToKafka()
		}
	}, 0, 1000)

	runApplication<ProducerApplication>(*args)
	run();
}


private fun run() {
	System.out.println("Setting up")

	// 1. Call the Twitter Client
	client = TwitterConfig.createTwitterClient(msgQueue, listOf("bolsonaro", "lula"))
	client!!.connect()

	// 2. Create Kafka Producer
	producer = KafkaProducerConfig.createKafkaProducer()


	// Shutdown Hook
	Runtime.getRuntime().addShutdownHook(Thread {
		System.out.println("Application is not stopping!")
		client!!.stop()
		System.out.println("Closing Producer")
		producer!!.close()
		System.out.println("Finished closing")
	})

	// 3. Send Tweets to Kafka
	while (!client!!.isDone) {
		var msg: String? = null
		try {
			msg = msgQueue.poll(5, TimeUnit.SECONDS)
		} catch (e: InterruptedException) {
			e.printStackTrace()
			client!!.stop()
		}
		if (msg != null) {
			if(msg.contains("lula")){
				var neutral = true;
				for (word in positive) {
					if (msg.indexOf(word) === -1) {
						map.put("Lula", Pair("positive", countPositiveLula++))
					}
					neutral = false;
				}
				for (word in negative) {
					if (msg.indexOf(word) === -1) {
						map.put("Lula", Pair("negative", countNegativeLula++))
					}
					neutral = false;
				}
				if (neutral){
					map.put("Lula", Pair("neutral", countNeutralLula++))
				}
			}

			if(msg.contains("bolsonaro")){
				var neutral = true;
				for (word in positive) {
					if (msg.indexOf(word) === -1) {
						map.put("Bolsonaro", Pair("positive", countPositiveBolsonaro++))
					}
					neutral = false;
				}
				for (word in negative) {
					if (msg.indexOf(word) === -1) {
						map.put("Bolsonaro", Pair("negative", countNegativeBolsonaro++))
					}
					neutral = false;
				}
				if (neutral){
					map.put("Bolsonaro", Pair("neutral", countNeutralBolsonaro++))
				}
			}
		}
		System.out.println(countPositiveLula)

	}
}

fun sendDataToKafka() {
	producer.use { producer ->
			val recordBuilder = GenericRecordBuilder(schema.getAvroSchema())
			recordBuilder.set("key_word", "lula_positive")
			recordBuilder.set("count", map.get("Lula")!!.takeIf { it.first == "positive" }?.second)
			val genericRecord: GenericRecord = recordBuilder.build()
			val producerRecord: ProducerRecord<String, GenericRecord> =
				ProducerRecord<String, GenericRecord>(
					TOPIC,
					UUID.randomUUID().toString(), genericRecord
				)
			producer!!.send(producerRecord)
			println("Published message")
	}
}





