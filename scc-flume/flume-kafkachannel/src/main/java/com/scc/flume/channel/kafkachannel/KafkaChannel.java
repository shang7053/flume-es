package com.scc.flume.channel.kafkachannel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
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
		return new KafkaTransaction();
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
				groupid = "flume_channel";
			}
			LOGGER.info("groupid={}", groupid);
			String capacity = context.getString("capacity");
			if ((capacity == null) || (capacity.isEmpty())) {
				capacity = "100";
			}
			if (Long.valueOf(capacity) > 1000) {
				capacity = "1000";
				LOGGER.warn("capacity MAX is 1000,now set capacity = 1000,because too MAX capacity will OOM!");
			}
			LOGGER.info("capacity={}", capacity);
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
			consumerPro.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
			consumerPro.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
			consumerPro.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, capacity);
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

	private class KafkaTransaction extends BasicTransactionSemantics {
		private Map<Integer, Long> lastOffsetMap = new HashMap<>();
		private Map<Integer, Long> startOffsetMap = new HashMap<>();
		private long dataSize;

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
				List<Map<String, Object>> datas = new ArrayList<>();
				KafkaChannel.this.records = KafkaChannel.this.consumer.poll(Long.MAX_VALUE);
				for (TopicPartition partition : KafkaChannel.this.records.partitions()) {
					List<ConsumerRecord<byte[], byte[]>> partitionRecords = KafkaChannel.this.records
							.records(partition);
					if (partitionRecords.size() == 0) {
						LOGGER.debug("partition={},no new data,continue next partition", partition);
						this.lastOffsetMap.put(partition.partition(), 0L);
						this.startOffsetMap.put(partition.partition(), 0L);
						continue;
					}
					for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
						datas.add(this.deserializeValue(record.value()));
					}
					long startOffset = partitionRecords.get(0).offset();
					long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
					this.lastOffsetMap.put(partition.partition(), lastOffset);
					this.startOffsetMap.put(partition.partition(), startOffset);
					LOGGER.debug("get data from partition={} startOffset={}，lastOffset={}",
							new Object[] { partition.partition(), startOffset, lastOffset });
				}
				this.dataSize = KafkaChannel.this.records.count();
				if (this.dataSize > 0) {
					return EventBuilder.withBody(this.serializeValue(datas), null);
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
			if (this.dataSize > 0) {
				LOGGER.debug("start commit!dataSize={}", this.dataSize);
				for (TopicPartition partition : KafkaChannel.this.records.partitions()) {
					long startOffset = this.startOffsetMap.get(partition.partition());
					long lastOffset = this.lastOffsetMap.get(partition.partition());
					LOGGER.debug("prepare Commit partition ={} ,startOffset={},lastOffset={}",
							new Object[] { partition.partition(), startOffset, lastOffset });
					KafkaChannel.this.consumer
							.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
					LOGGER.info("commit partition ={} ,offset to lastOffset={} success! commit size ={}",
							new Object[] { partition.partition(), lastOffset, this.dataSize });
				}
			}
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
			if (this.dataSize > 0) {
				LOGGER.debug("start doRollback!dataSize={}", this.dataSize);
				// KafkaChannel.this.consumer.commitSync();
				for (TopicPartition partition : KafkaChannel.this.records.partitions()) {
					long startOffset = this.startOffsetMap.get(partition.partition());
					long lastOffset = this.lastOffsetMap.get(partition.partition());
					LOGGER.debug("prepare Rollback partition ={} ,startOffset={},lastOffset={}",
							new Object[] { partition.partition(), startOffset, lastOffset });
					if (lastOffset - startOffset == 0) {
						LOGGER.debug("partition ={} no data need commit!,continue next partition!",
								new Object[] { partition.partition() });
						continue;
					}
					KafkaChannel.this.consumer.seek(partition, startOffset);
					// .commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(startOffset)));
					LOGGER.info("doRollback rollbak partition ={} ,offset to startOffset={} success!commit size ={}",
							new Object[] { partition.partition(), startOffset, this.dataSize });
				}
			}
		}

		private byte[] serializeValue(Event event) throws IOException {
			Map<String, Object> data = new HashMap<>();
			data.put("headers", event.getHeaders());
			data.put("body", event.getBody());
			ByteArrayOutputStream obj = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(obj);
			out.writeObject(data);
			return obj.toByteArray();
		}

		private byte[] serializeValue(List<Map<String, Object>> datas) throws IOException {
			ByteArrayOutputStream obj = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(obj);
			out.writeObject(datas);
			return obj.toByteArray();
		}

		private Map<String, Object> deserializeValue(byte[] value) throws IOException, ClassNotFoundException {
			ByteArrayInputStream bin = new ByteArrayInputStream(value);
			ObjectInputStream obin = new ObjectInputStream(bin);
			return (Map<String, Object>) obin.readObject();
		}
	}

}
