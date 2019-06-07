package com.df.zafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainKafka {

	private static Logger logger;
	private static CountDownLatch QUIT_LOOK = new CountDownLatch(1);
	private static AtomicBoolean KEEP_ALIVE = new AtomicBoolean(true);
	private static final String TOPIC_A = "Topic-A";
	
	private static String getServer() {
		return "localhost:9092";
	}
	
	private static String getClient() {
		return "client-1";
	}

	private static String getGroup() {
		return "group-1";
	}
	
	
	private static Properties createProducerProps() {
		
		Properties propsProducer = new Properties();
		
		propsProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getServer());
        propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        propsProducer.put(ProducerConfig.CLIENT_ID_CONFIG, getClient());
        propsProducer.put(ProducerConfig.ACKS_CONFIG, "all");
        propsProducer.put(ProducerConfig.RETRIES_CONFIG , 5);
        
        return propsProducer;
	}
	
	private static Properties createConsumerProps() {
		
		Properties propsConsumer = new Properties();
		
		propsConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getServer());
		propsConsumer.put(ConsumerConfig.CLIENT_ID_CONFIG, getClient());
		propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, getGroup());
		propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
		propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        
        return propsConsumer;
	}
	
	private static void startProducer() {
		
		logger.info("Starting Producer");
		try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(createProducerProps())) {
			while (KEEP_ALIVE.get()) {
				logger.info("Producing...");
				ProducerRecord<String, String> record = 
						new ProducerRecord<String, String>(
								TOPIC_A, "TS", Long.toHexString(System.currentTimeMillis()));
				producer.send(record);
				
				QUIT_LOOK.await(30, TimeUnit.SECONDS);
			}
		} catch (Exception e) {
			logger.error("Error on producer", e);
		} 
	}
	
	private static void startConsumer() {
		logger.info("Init Consumer...");
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(createConsumerProps())) {
			logger.info("Consuming");
			consumer.subscribe(Arrays.asList(new String[] {TOPIC_A}));
			while (KEEP_ALIVE.get()) {
				logger.info("Polling");
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
				for (ConsumerRecord<String, String> record : records) 
		        {
					long ts = Long.parseLong(record.value(), 16);
					long diff = System.currentTimeMillis() - ts;
					logger.info(String.format("topic = %s, partition = %d, offset = %d, key = %s, value = %s, diff = %d",
			                record.topic(), record.partition(), record.offset(),
			                record.key(), record.value(), diff));
		        }
			}
			consumer.unsubscribe();
		}
	}
	
	private static void startStream() {
		
//		Topology topology = new Topology();
//		final StreamsBuilder builder = new StreamsBuilder();
//		KStream<String, String> kStream = builder.stream(TOPIC_A);
//		
//		KafkaStreams streams = new KafkaStreams(topology, createStreamProps());
	}
	
	public static void main(String[] args) {
		
		logger = LoggerFactory.getLogger(MainKafka.class);
		try {
			Thread tConsumer = new Thread(new Runnable() {
				@Override
				public void run() {
					startConsumer();
				}
			});
			
			Thread tProducer = new Thread(new Runnable() {
				@Override
				public void run() {
					startProducer();
				}
			});
			
			Thread tStream = new Thread(new Runnable() {
				@Override
				public void run() {
					startStream();
				}
			});
			
			tConsumer.start();
			tProducer.start();
			tStream.start();
			
			while (KEEP_ALIVE.get()) {
				logger.debug("Looping...");
				QUIT_LOOK.await(30, TimeUnit.SECONDS);
			}
			logger.debug("Exiting");
		} catch (Exception e) {
			logger.error("Error connecting to Kafka service: {}", e);
		}
	}

}
