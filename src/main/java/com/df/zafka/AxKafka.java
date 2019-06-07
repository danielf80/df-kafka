package com.df.zafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AxKafka {
	private static Logger logger;
	private static CountDownLatch QUIT_LOOK = new CountDownLatch(1);
	private static AtomicBoolean KEEP_ALIVE = new AtomicBoolean(true);
	private static final String TOPIC_A = "switch-inbound-mt";
	
	private static String getServer() {
		return "localhost:9092";
	}
	
	private static String getClient() {
		return "client-1";
	}

	private static String getGroup() {
		return "group-1";
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
					logger.info(String.format("topic = %s, partition = %d, offset = %d, key = %s, value = %s",
			                record.topic(), record.partition(), record.offset(),
			                record.key(), record.value()));
		        }
			}
			consumer.unsubscribe();
		} finally {
			KEEP_ALIVE.set(false);
		}
	}
	
	private static void startListTopics() {
		logger.info("Init Consumer...");
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(createConsumerProps())) {
			Map<String, List<PartitionInfo>> topics = consumer.listTopics();
			Set<String> keys = topics.keySet();
			for (String key : keys)
				logger.info("Topic '{}'", key);
		}
		KEEP_ALIVE.set(false);
		QUIT_LOOK.countDown();
	}
	
	
	public static void main(String[] args) {
		
		int[][] arr = null;
		
		
		logger = LoggerFactory.getLogger(MainKafka.class);
		try {
			Thread tConsumer = new Thread(new Runnable() {
				@Override
				public void run() {
					startConsumer();
//					startListTopics();
				}
			});
			
			tConsumer.start();
			
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
