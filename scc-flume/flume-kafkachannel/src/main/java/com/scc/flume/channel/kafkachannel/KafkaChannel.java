package com.scc.flume.channel.kafkachannel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Properties;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName: KafkaChannel2
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author shangchengcai@voole.com
 * @date 2018年1月19日 下午2:27:43
 * 
 */
public class KafkaChannel extends BasicChannelSemantics {
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaChannel.class);
	private String topic;
	private KafkaProducer<byte[], byte[]> kafkaProducer;
	private Consumer<byte[], byte[]> consumer;
	private ConsumerRecords<byte[], byte[]> records;

	/*
	 * (非 Javadoc) <p>Title: createTransaction</p> <p>Description: </p>
	 * 
	 * @return
	 * 
	 * @see org.apache.flume.channel.BasicChannelSemantics#createTransaction()
	 */
	@Override
	protected BasicTransactionSemantics createTransaction() {
		return new NullTransaction();
	}

	/*
	 * (非 Javadoc) <p>Title: configure</p> <p>Description: </p>
	 * 
	 * @param context
	 * 
	 * @see org.apache.flume.channel.AbstractChannel#configure(org.apache.flume.Context)
	 */
	@Override
	public void configure(Context context) {
		LOGGER.info("=========================start config kafka channel==============================");
		try {
			super.configure(context);
			// config kakfa producer and consumer
			String bootstrapServers = context.getString("bootstrap.servers");
			if ((bootstrapServers == null) || (bootstrapServers.isEmpty())) {
				throw new ConfigurationException("Bootstrap Servers must be specified");
			}
			LOGGER.info("bootstrapServers={}", bootstrapServers);
			this.topic = context.getString("topic");
			if ((this.topic == null) || (this.topic.isEmpty())) {
				throw new ConfigurationException("topic must be specified");
			}
			LOGGER.info("topic={}", this.topic);
			String groupid = context.getString("groupid");
			if ((groupid == null) || (groupid.isEmpty())) {
				groupid = "flume";
			}
			LOGGER.info("groupid={}", groupid);
			LOGGER.info("create producer client……");
			// create producer client
			Properties producerPro = new Properties();
			producerPro.put("bootstrap.servers", bootstrapServers);
			producerPro.put("acks", "1");
			producerPro.put("retries", 0);
			producerPro.put("batch.size", 16384);
			producerPro.put("linger.ms", 1);
			producerPro.put("buffer.memory", 33554432);
			producerPro.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
			producerPro.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
			this.kafkaProducer = new KafkaProducer<byte[], byte[]>(producerPro);

			LOGGER.info("create consumer client……");
			// create consumer client
			Properties consumerPro = new Properties();
			consumerPro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			consumerPro.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);
			consumerPro.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
			consumerPro.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
			consumerPro.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
			consumerPro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			consumerPro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			this.consumer = new KafkaConsumer<>(consumerPro);
			this.consumer.subscribe(Arrays.asList(this.topic));
			this.records = this.consumer.poll(1);
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage(), e);
			System.exit(1);
		}
		LOGGER.info("=========================end config kafka channel==============================");
	}

	/*
	 * (非 Javadoc) <p>Title: stop</p> <p>Description: </p>
	 * 
	 * @see org.apache.flume.channel.AbstractChannel#stop()
	 */
	@Override
	public synchronized void stop() {
		super.stop();
		this.kafkaProducer.close();
		this.consumer.close();
	}

	private class NullTransaction extends BasicTransactionSemantics {
		/*
		 * (非 Javadoc) <p>Title: doPut</p> <p>Description: </p>
		 * 
		 * @param event
		 * 
		 * @throws InterruptedException
		 * 
		 * @see org.apache.flume.channel.BasicTransactionSemantics#doPut(org.apache.flume.Event)
		 */
		@Override
		protected void doPut(Event event) throws InterruptedException {
			LOGGER.debug("-------------------------put channel data to kafka start-------------------------");
			try {
				ProducerRecord<byte[], byte[]> kafkaRecord = new ProducerRecord<byte[], byte[]>(KafkaChannel.this.topic,
						this.serializeValue(event));
				KafkaChannel.this.kafkaProducer.send(kafkaRecord, new Callback() {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if (null != e) {
							LOGGER.error(e.getMessage(), e);
							e.printStackTrace();
						}
						LOGGER.debug("put channel data to kafka complete!");
					}
				});
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.error(e.getMessage(), e);
			}
			LOGGER.debug("-------------------------put channel data to kafka end-------------------------");
		}

		/*
		 * (非 Javadoc) <p>Title: doTake</p> <p>Description: </p>
		 * 
		 * @return
		 * 
		 * @throws InterruptedException
		 * 
		 * @see org.apache.flume.channel.BasicTransactionSemantics#doTake()
		 */
		@Override
		protected Event doTake() throws InterruptedException {
			LOGGER.debug("-------------------------take channel data start-------------------------");
			try {
				KafkaChannel.this.consumer.subscribe(Arrays.asList(KafkaChannel.this.topic));
				KafkaChannel.this.records = KafkaChannel.this.consumer.poll(1);
				for (ConsumerRecord<byte[], byte[]> record : KafkaChannel.this.records) {
					return this.deserializeValue(record.value());
				}
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.error(e.getMessage(), e);
			}
			LOGGER.debug("-------------------------take channel data end-------------------------");
			return null;
		}

		/*
		 * (非 Javadoc) <p>Title: doCommit</p> <p>Description: </p>
		 * 
		 * @throws InterruptedException
		 * 
		 * @see org.apache.flume.channel.BasicTransactionSemantics#doCommit()
		 */
		@Override
		protected void doCommit() throws InterruptedException {
			LOGGER.debug("kakfa auto commit,don't need call doCommit for commit!");
		}

		/*
		 * (非 Javadoc) <p>Title: doRollback</p> <p>Description: </p>
		 * 
		 * @throws InterruptedException
		 * 
		 * @see org.apache.flume.channel.BasicTransactionSemantics#doRollback()
		 */
		@Override
		protected void doRollback() throws InterruptedException {
			LOGGER.debug("kakfa auto commit,don't need call doRollback for Rollback!");
		}

		private byte[] serializeValue(Event event) throws IOException {
			LogData data = new LogData();
			data.setHeaders(event.getHeaders());
			data.setBody(event.getBody());
			ByteArrayOutputStream obj = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(obj);
			out.writeObject(data);
			return obj.toByteArray();
		}

		private Event deserializeValue(byte[] value) throws IOException, ClassNotFoundException {
			ByteArrayInputStream bin = new ByteArrayInputStream(value);
			ObjectInputStream obin = new ObjectInputStream(bin);
			LogData data = (LogData) obin.readObject();
			return EventBuilder.withBody(data.getBody(), data.getHeaders());
		}
	}

}
