package com.twitter.producer

import com.twitter.hbc.core.Client
import com.twitter.producer.config.KafkaProducerConfig
import com.twitter.producer.config.TwitterConfig
import org.apache.commons.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit


//private val logger: Logger = LoggerFactory.getLogger(ProducerApplication::class.java)

var countPositiveLula = 0;
var countNegativeLula = 0;
var countNeutralLula = 0;

var countPositiveBolsonaro = 0;
var countNegativeBolsonaro = 0;
var countNeutralBolsonaro = 0;

var positive = arrayOf("bom", "honesto", "feliz", "educacao", "povo", "bem")
var negative = arrayOf("corrupto", "ladr\\u00e3o", "preso", "corrup\\u00e7\\u00e3o", "crime", "criminoso")

private var client: Client? = null
private var producer: KafkaProducer<String, String>? = null
private val msgQueue: BlockingQueue<String> = LinkedBlockingQueue(100)

@SpringBootApplication
class ProducerApplication

fun main(args: Array<String>) {
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
				//Processa mensagens positivas
				for (word in positive) {
					if (msg.indexOf(word) === -1) {
						countPositiveLula++;
					}
					neutral = false;
				}
				//Processa mensagesn negativas
				for (word in negative) {
					if (msg.indexOf(word) === -1) {
						countNegativeLula++;
					}
					neutral = false;
				}
				//Processa mensagens neutras
				if (neutral){
					countNeutralLula++;
				}
			}

			if(msg.contains("bolsonaro")){
				var neutral = true;
				//Processa mensagens positivas
				for (word in positive) {
					if (msg.indexOf(word) === -1) {
						countPositiveBolsonaro++;
					}
					neutral = false;
				}
				//Processa mensagesn negativas
				for (word in negative) {
					if (msg.indexOf(word) === -1) {
						countNegativeBolsonaro++;
					}
					neutral = false;
				}
				//Processa mensagens neutras
				if (neutral){
					countNeutralBolsonaro++;
				}
			}
		}
		System.out.println(countPositiveLula)

		//if (msg != null) {
		//	producer!!.send(ProducerRecord(TOPIC, null, msg)) { _, e ->
		//		if (e != null) {
		//			System.out.println("Some error OR something bad happened")
		//		}
		//	}
		//}
	}
}

//make a function scheduled to run each 10 seconds to send message to kafka



