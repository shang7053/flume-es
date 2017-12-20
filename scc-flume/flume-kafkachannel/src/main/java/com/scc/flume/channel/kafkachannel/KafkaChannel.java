package com.scc.flume.channel.kafkachannel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.LogPrivacyUtil;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.kafka.KafkaChannelCounter;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import scala.Option;
import scala.collection.JavaConverters;

/**
 * @ClassName: KafkaChannel
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author shangchengcai@voole.com
 * @date 2017年12月20日 下午4:59:25
 * 
 */
public class KafkaChannel extends BasicChannelSemantics {

	private static final Logger logger = LoggerFactory.getLogger(KafkaChannel.class);
	private static final int ZK_SESSION_TIMEOUT = 30000;
	private static final int ZK_CONNECTION_TIMEOUT = 30000;
	private final Properties consumerProps = new Properties();
	private final Properties producerProps = new Properties();
	private KafkaProducer<String, byte[]> producer;
	private final String channelUUID = UUID.randomUUID().toString();
	private AtomicReference<String> topic = new AtomicReference();
	private boolean parseAsFlumeEvent = true;
	private String zookeeperConnect = null;
	private String topicStr = "flume-channel";
	private String groupId = "flume";
	private String partitionHeader = null;
	private Integer staticPartitionId;
	private boolean migrateZookeeperOffsets = true;
	AtomicBoolean rebalanceFlag = new AtomicBoolean();
	private long pollTimeout = 500L;
	private final List<ConsumerAndRecords> consumers = Collections.synchronizedList(new LinkedList());
	private KafkaChannelCounter counter;
	private final ThreadLocal<ConsumerAndRecords> consumerAndRecords = new ThreadLocal() {
		@Override
		public KafkaChannel.ConsumerAndRecords initialValue() {
			return KafkaChannel.this.createConsumerAndRecords();
		}
	};

	@Override
	public void start() {
		logger.info("Starting Kafka Channel: {}", this.getName());
		if ((this.migrateZookeeperOffsets) && (this.zookeeperConnect != null) && (!this.zookeeperConnect.isEmpty())) {
			this.migrateOffsets();
		}
		this.producer = new KafkaProducer(this.producerProps);

		logger.info("Topic = {}", this.topic.get());
		this.counter.start();
		super.start();
	}

	@Override
	public void stop() {
		for (ConsumerAndRecords c : this.consumers) {
			try {
				this.decommissionConsumerAndRecords(c);
			} catch (Exception ex) {
				logger.warn("Error while shutting down consumer.", ex);
			}
		}
		this.producer.close();
		this.counter.stop();
		super.stop();
		logger.info("Kafka channel {} stopped.", this.getName());
	}

	@Override
	protected BasicTransactionSemantics createTransaction() {
		return new KafkaTransaction();
	}

	@Override
	public void configure(Context ctx) {
		this.translateOldProps(ctx);

		this.topicStr = ctx.getString("kafka.topic");
		if ((this.topicStr == null) || (this.topicStr.isEmpty())) {
			this.topicStr = "flume-channel";
			logger.info("Topic was not specified. Using {} as the topic.", this.topicStr);
		}
		this.topic.set(this.topicStr);

		this.groupId = ctx.getString("kafka.consumer.group.id");
		if ((this.groupId == null) || (this.groupId.isEmpty())) {
			this.groupId = "flume";
			logger.info("Group ID was not specified. Using {} as the group id.", this.groupId);
		}
		String bootStrapServers = ctx.getString("kafka.bootstrap.servers");
		if ((bootStrapServers == null) || (bootStrapServers.isEmpty())) {
			throw new ConfigurationException("Bootstrap Servers must be specified");
		}
		this.setProducerProps(ctx, bootStrapServers);
		this.setConsumerProps(ctx, bootStrapServers);

		this.parseAsFlumeEvent = ctx.getBoolean("parseAsFlumeEvent", Boolean.valueOf(true)).booleanValue();
		this.pollTimeout = ctx.getLong("kafka.pollTimeout", Long.valueOf(500L)).longValue();

		this.staticPartitionId = ctx.getInteger("defaultPartitionId");
		this.partitionHeader = ctx.getString("partitionIdHeader");

		this.migrateZookeeperOffsets = ctx.getBoolean("migrateZookeeperOffsets", Boolean.valueOf(true)).booleanValue();
		this.zookeeperConnect = ctx.getString("zookeeperConnect");
		if ((logger.isDebugEnabled()) && (LogPrivacyUtil.allowLogPrintConfig())) {
			logger.debug("Kafka properties: {}", ctx);
		}
		if (this.counter == null) {
			this.counter = new KafkaChannelCounter(this.getName());
		}
	}

	private void translateOldProps(Context ctx) {
		if (!ctx.containsKey("kafka.topic")) {
			ctx.put("kafka.topic", ctx.getString("topic"));
			logger.warn("{} is deprecated. Please use the parameter {}", "topic", "kafka.topic");
		}
		if (!ctx.containsKey("kafka.bootstrap.servers")) {
			String brokerList = ctx.getString("brokerList");
			if ((brokerList == null) || (brokerList.isEmpty())) {
				throw new ConfigurationException("Bootstrap Servers must be specified");
			}
			ctx.put("kafka.bootstrap.servers", brokerList);
			logger.warn("{} is deprecated. Please use the parameter {}", "brokerList", "kafka.bootstrap.servers");
		}
		if (!ctx.containsKey("kafka.consumer.group.id")) {
			String oldGroupId = ctx.getString("groupId");
			if ((oldGroupId != null) && (!oldGroupId.isEmpty())) {
				ctx.put("kafka.consumer.group.id", oldGroupId);
				logger.warn("{} is deprecated. Please use the parameter {}", "groupId", "kafka.consumer.group.id");
			}
		}
		if (!ctx.containsKey("kafka.consumer.auto.offset.reset")) {
			Boolean oldReadSmallest = ctx.getBoolean("readSmallestOffset");
			if (oldReadSmallest != null) {
				String auto;
				if (oldReadSmallest.booleanValue()) {
					auto = "earliest";
				} else {
					auto = "latest";
				}
				ctx.put("kafka.consumer.auto.offset.reset", auto);
				logger.warn("{} is deprecated. Please use the parameter {}", "readSmallestOffset",
						"kafka.consumer.auto.offset.reset");
			}
		}
	}

	private void setProducerProps(Context ctx, String bootStrapServers) {
		this.producerProps.clear();
		this.producerProps.put("acks", "all");
		this.producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		this.producerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		this.producerProps.putAll(ctx.getSubProperties("kafka.producer."));
		this.producerProps.put("bootstrap.servers", bootStrapServers);
	}

	protected Properties getProducerProps() {
		return this.producerProps;
	}

	private void setConsumerProps(Context ctx, String bootStrapServers) {
		this.consumerProps.clear();
		this.consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		this.consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		this.consumerProps.put("auto.offset.reset", "earliest");

		this.consumerProps.putAll(ctx.getSubProperties("kafka.consumer."));

		this.consumerProps.put("bootstrap.servers", bootStrapServers);
		this.consumerProps.put("group.id", this.groupId);
		this.consumerProps.put("enable.auto.commit", Boolean.valueOf(false));
	}

	protected Properties getConsumerProps() {
		return this.consumerProps;
	}

	private synchronized ConsumerAndRecords createConsumerAndRecords() {
		try {
			KafkaConsumer<String, byte[]> consumer = new KafkaConsumer(this.consumerProps);
			ConsumerAndRecords car = new ConsumerAndRecords(consumer, this.channelUUID);
			logger.info("Created new consumer to connect to Kafka");
			car.consumer.subscribe(Arrays.asList(new String[] { this.topic.get() }),
					new ChannelRebalanceListener(this.rebalanceFlag));

			car.offsets = new HashMap();
			this.consumers.add(car);
			return car;
		} catch (Exception e) {
			throw new FlumeException("Unable to connect to Kafka", e);
		}
	}

	private void migrateOffsets() {
		ZkUtils zkUtils = ZkUtils.apply(this.zookeeperConnect, 30000, 30000, JaasUtils.isZkSecurityEnabled());
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer(this.consumerProps);
		try {
			Map<TopicPartition, OffsetAndMetadata> kafkaOffsets = this.getKafkaOffsets(consumer);
			if (!kafkaOffsets.isEmpty()) {
				logger.info("Found Kafka offsets for topic {}. Will not migrate from zookeeper", this.topicStr);
				logger.debug("Offsets found: {}", kafkaOffsets);
				return;
			}
			logger.info("No Kafka offsets found. Migrating zookeeper offsets");
			Map<TopicPartition, OffsetAndMetadata> zookeeperOffsets = this.getZookeeperOffsets(zkUtils);
			if (zookeeperOffsets.isEmpty()) {
				logger.warn("No offsets to migrate found in Zookeeper");
				return;
			}
			logger.info("Committing Zookeeper offsets to Kafka");
			logger.debug("Offsets to commit: {}", zookeeperOffsets);
			consumer.commitSync(zookeeperOffsets);

			Map<TopicPartition, OffsetAndMetadata> newKafkaOffsets = this.getKafkaOffsets(consumer);
			logger.debug("Offsets committed: {}", newKafkaOffsets);
			if (!newKafkaOffsets.keySet().containsAll(zookeeperOffsets.keySet())) {
				throw new FlumeException("Offsets could not be committed");
			}
		} finally {
			zkUtils.close();
			consumer.close();
		}
	}

	private Map<TopicPartition, OffsetAndMetadata> getKafkaOffsets(KafkaConsumer<String, byte[]> client) {
		Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap();
		List<PartitionInfo> partitions = client.partitionsFor(this.topicStr);
		for (PartitionInfo partition : partitions) {
			TopicPartition key = new TopicPartition(this.topicStr, partition.partition());
			OffsetAndMetadata offsetAndMetadata = client.committed(key);
			if (offsetAndMetadata != null) {
				offsets.put(key, offsetAndMetadata);
			}
		}
		return offsets;
	}

	private Map<TopicPartition, OffsetAndMetadata> getZookeeperOffsets(ZkUtils client) {
		Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap();
		ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(this.groupId, this.topicStr);

		Collection<String> partitions = JavaConverters
				.asJavaCollectionConverter(client.getChildrenParentMayNotExist(topicDirs.consumerOffsetDir()))
				.asJavaCollection();
		for (String partition : partitions) {
			TopicPartition key = new TopicPartition(this.topicStr, Integer.valueOf(partition).intValue());

			Option<String> data = client.readDataMaybeNull(topicDirs.consumerOffsetDir() + "/" + partition)._1();
			if (data.isDefined()) {
				Long offset = Long.valueOf(data.get());
				offsets.put(key, new OffsetAndMetadata(offset.longValue()));
			}
		}
		return offsets;
	}

	private void decommissionConsumerAndRecords(ConsumerAndRecords c) {
		c.consumer.wakeup();
		c.consumer.close();
	}

	@VisibleForTesting
	void registerThread() {
		try {
			this.consumerAndRecords.get();
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}

	private static enum TransactionType {
		PUT, TAKE, NONE;

		private TransactionType() {
		}
	}

	private class KafkaTransaction extends BasicTransactionSemantics {
		private KafkaChannel.TransactionType type = KafkaChannel.TransactionType.NONE;
		private Optional<ByteArrayOutputStream> tempOutStream = Optional.absent();
		private Optional<LinkedList<ProducerRecord<String, byte[]>>> producerRecords = Optional.absent();
		private Optional<LinkedList<Event>> events = Optional.absent();
		private Optional<SpecificDatumWriter<AvroFlumeEvent>> writer = Optional.absent();
		private Optional<SpecificDatumReader<AvroFlumeEvent>> reader = Optional.absent();
		private Optional<LinkedList<Future<RecordMetadata>>> kafkaFutures = Optional.absent();
		private final String batchUUID = UUID.randomUUID().toString();
		private BinaryEncoder encoder = null;
		private BinaryDecoder decoder = null;
		private boolean eventTaken = false;

		private KafkaTransaction() {
		}

		@Override
		protected void doBegin() throws InterruptedException {
			KafkaChannel.this.rebalanceFlag.set(false);
		}

		@Override
		protected void doPut(Event event) throws InterruptedException {
			this.type = KafkaChannel.TransactionType.PUT;
			if (!this.producerRecords.isPresent()) {
				this.producerRecords = Optional.of(new LinkedList());
			}
			String key = event.getHeaders().get("key");

			Integer partitionId = null;
			try {
				if (KafkaChannel.this.staticPartitionId != null) {
					partitionId = KafkaChannel.this.staticPartitionId;
				}
				if (KafkaChannel.this.partitionHeader != null) {
					String headerVal = event.getHeaders().get(KafkaChannel.this.partitionHeader);
					if (headerVal != null) {
						partitionId = Integer.valueOf(Integer.parseInt(headerVal));
					}
				}
				if (partitionId != null) {
					((LinkedList) this.producerRecords.get()).add(new ProducerRecord(KafkaChannel.this.topic.get(),
							partitionId, key, this.serializeValue(event, KafkaChannel.this.parseAsFlumeEvent)));
				} else {
					((LinkedList) this.producerRecords.get()).add(new ProducerRecord(KafkaChannel.this.topic.get(), key,
							this.serializeValue(event, KafkaChannel.this.parseAsFlumeEvent)));
				}
			} catch (NumberFormatException e) {
				throw new ChannelException("Non integer partition id specified", e);
			} catch (Exception e) {
				throw new ChannelException("Error while serializing event", e);
			}
		}

		@Override
		protected Event doTake() throws InterruptedException {
			KafkaChannel.logger.trace("Starting event take");
			this.type = KafkaChannel.TransactionType.TAKE;
			try {
				if (!KafkaChannel.this.consumerAndRecords.get().uuid.equals(KafkaChannel.this.channelUUID)) {
					KafkaChannel.logger.info("UUID mismatch, creating new consumer");
					KafkaChannel.this.decommissionConsumerAndRecords(KafkaChannel.this.consumerAndRecords.get());
					KafkaChannel.this.consumerAndRecords.remove();
				}
			} catch (Exception ex) {
				KafkaChannel.logger.warn("Error while shutting down consumer", ex);
			}
			if (!this.events.isPresent()) {
				this.events = Optional.of(new LinkedList());
			}
			if (KafkaChannel.this.rebalanceFlag.get()) {
				KafkaChannel.logger.debug("Returning null event after Consumer rebalance.");
				return null;
			}
			Event e;
			if (!KafkaChannel.this.consumerAndRecords.get().failedEvents.isEmpty()) {
				e = KafkaChannel.this.consumerAndRecords.get().failedEvents.removeFirst();
			} else {
				if (KafkaChannel.logger.isTraceEnabled()) {
					KafkaChannel.logger.trace("Assignment during take: {}",
							KafkaChannel.this.consumerAndRecords.get().consumer.assignment().toString());
				}
				try {
					long startTime = System.nanoTime();
					if (!KafkaChannel.this.consumerAndRecords.get().recordIterator.hasNext()) {
						KafkaChannel.this.consumerAndRecords.get().poll();
					}
					if (KafkaChannel.this.consumerAndRecords.get().recordIterator.hasNext()) {
						ConsumerRecord<String, byte[]> record = KafkaChannel.this.consumerAndRecords
								.get().recordIterator.next();
						e = this.deserializeValue(record.value(), KafkaChannel.this.parseAsFlumeEvent);
						TopicPartition tp = new TopicPartition(record.topic(), record.partition());
						OffsetAndMetadata oam = new OffsetAndMetadata(record.offset() + 1L, this.batchUUID);
						KafkaChannel.this.consumerAndRecords.get().saveOffsets(tp, oam);
						if (record.key() != null) {
							e.getHeaders().put("key", record.key());
						}
						long endTime = System.nanoTime();
						KafkaChannel.this.counter.addToKafkaEventGetTimer((endTime - startTime) / 1000000L);
						if (KafkaChannel.logger.isDebugEnabled()) {
							KafkaChannel.logger.debug("{} processed output from partition {} offset {}",
									new Object[] { KafkaChannel.this.getName(), Integer.valueOf(record.partition()),
											Long.valueOf(record.offset()) });
						}
					} else {
						return null;
					}
				} catch (Exception ex) {
					KafkaChannel.logger.warn(
							"Error while getting events from Kafka. This is usually caused by trying to read a non-flume event. Ensure the setting for parseAsFlumeEvent is correct",
							ex);

					throw new ChannelException("Error while getting events from Kafka", ex);
				}
			}
			this.eventTaken = true;
			((LinkedList) this.events.get()).add(e);
			return e;
		}

		@Override
		protected void doCommit() throws InterruptedException {
			KafkaChannel.logger.trace("Starting commit");
			if (this.type.equals(KafkaChannel.TransactionType.NONE)) {
				return;
			}
			if (this.type.equals(KafkaChannel.TransactionType.PUT)) {
				if (!this.kafkaFutures.isPresent()) {
					this.kafkaFutures = Optional.of(new LinkedList());
				}
				try {
					long batchSize = ((LinkedList) this.producerRecords.get()).size();
					long startTime = System.nanoTime();
					int index = 0;
					for (ProducerRecord<String, byte[]> record : this.producerRecords.get()) {
						index++;
						((LinkedList) this.kafkaFutures.get())
								.add(KafkaChannel.this.producer.send(record, new ChannelCallback(index, startTime)));
					}
					KafkaChannel.this.producer.flush();
					for (Future<RecordMetadata> future : this.kafkaFutures.get()) {
						future.get();
					}
					long endTime = System.nanoTime();
					KafkaChannel.this.counter.addToKafkaEventSendTimer((endTime - startTime) / 1000000L);
					KafkaChannel.this.counter.addToEventPutSuccessCount(batchSize);
					((LinkedList) this.producerRecords.get()).clear();
					((LinkedList) this.kafkaFutures.get()).clear();
				} catch (Exception ex) {
					KafkaChannel.logger.warn("Sending events to Kafka failed", ex);
					throw new ChannelException("Commit failed as send to Kafka failed", ex);
				}
			} else {
				if ((KafkaChannel.this.consumerAndRecords.get().failedEvents.isEmpty()) && (this.eventTaken)) {
					KafkaChannel.logger.trace("About to commit batch");
					long startTime = System.nanoTime();
					KafkaChannel.this.consumerAndRecords.get().commitOffsets();
					long endTime = System.nanoTime();
					KafkaChannel.this.counter.addToKafkaCommitTimer((endTime - startTime) / 1000000L);
					if (KafkaChannel.logger.isDebugEnabled()) {
						KafkaChannel.logger
								.debug(KafkaChannel.this.consumerAndRecords.get().getCommittedOffsetsString());
					}
				}
				int takes = ((LinkedList) this.events.get()).size();
				if (takes > 0) {
					KafkaChannel.this.counter.addToEventTakeSuccessCount(takes);
					((LinkedList) this.events.get()).clear();
				}
			}
		}

		@Override
		protected void doRollback() throws InterruptedException {
			if (this.type.equals(KafkaChannel.TransactionType.NONE)) {
				return;
			}
			if (this.type.equals(KafkaChannel.TransactionType.PUT)) {
				((LinkedList) this.producerRecords.get()).clear();
				((LinkedList) this.kafkaFutures.get()).clear();
			} else {
				KafkaChannel.this.counter.addToRollbackCounter(((LinkedList) this.events.get()).size());
				KafkaChannel.this.consumerAndRecords.get().failedEvents.addAll(this.events.get());
				((LinkedList) this.events.get()).clear();
			}
		}

		private byte[] serializeValue(Event event, boolean parseAsFlumeEvent) throws IOException {
			byte[] bytes;
			if (parseAsFlumeEvent) {
				if (!this.tempOutStream.isPresent()) {
					this.tempOutStream = Optional.of(new ByteArrayOutputStream());
				}
				if (!this.writer.isPresent()) {
					this.writer = Optional.of(new SpecificDatumWriter(AvroFlumeEvent.class));
				}
				this.tempOutStream.get().reset();

				AvroFlumeEvent e = new AvroFlumeEvent(KafkaChannel.toCharSeqMap(event.getHeaders()),
						ByteBuffer.wrap(event.getBody()));

				this.encoder = EncoderFactory.get().directBinaryEncoder(this.tempOutStream.get(), this.encoder);
				((SpecificDatumWriter) this.writer.get()).write(e, this.encoder);
				this.encoder.flush();
				bytes = this.tempOutStream.get().toByteArray();
			} else {
				bytes = event.getBody();
			}
			return bytes;
		}

		private Event deserializeValue(byte[] value, boolean parseAsFlumeEvent) throws IOException {
			Event e;
			if (parseAsFlumeEvent) {
				ByteArrayInputStream in = new ByteArrayInputStream(value);

				this.decoder = DecoderFactory.get().directBinaryDecoder(in, this.decoder);
				if (!this.reader.isPresent()) {
					this.reader = Optional.of(new SpecificDatumReader(AvroFlumeEvent.class));
				}
				AvroFlumeEvent event = (AvroFlumeEvent) ((SpecificDatumReader) this.reader.get()).read(null,
						this.decoder);
				e = EventBuilder.withBody(event.getBody().array(), KafkaChannel.toStringMap(event.getHeaders()));
			} else {
				e = EventBuilder.withBody(value, Collections.EMPTY_MAP);
			}
			return e;
		}
	}

	private static Map<CharSequence, CharSequence> toCharSeqMap(Map<String, String> stringMap) {
		Map<CharSequence, CharSequence> charSeqMap = new HashMap();
		for (Map.Entry<String, String> entry : stringMap.entrySet()) {
			charSeqMap.put(entry.getKey(), entry.getValue());
		}
		return charSeqMap;
	}

	private static Map<String, String> toStringMap(Map<CharSequence, CharSequence> charSeqMap) {
		Map<String, String> stringMap = new HashMap();
		for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
			stringMap.put(entry.getKey().toString(), entry.getValue().toString());
		}
		return stringMap;
	}

	private class ConsumerAndRecords {
		final KafkaConsumer<String, byte[]> consumer;
		final String uuid;
		final LinkedList<Event> failedEvents = new LinkedList();
		ConsumerRecords<String, byte[]> records;
		Iterator<ConsumerRecord<String, byte[]>> recordIterator;
		Map<TopicPartition, OffsetAndMetadata> offsets;

		ConsumerAndRecords(KafkaConsumer<String, byte[]> consumer, String uuid) {
			this.consumer = consumer;
			this.uuid = uuid;
			this.records = ConsumerRecords.empty();
			this.recordIterator = this.records.iterator();
		}

		private void poll() {
			KafkaChannel.logger.trace("Polling with timeout: {}ms channel-{}",
					Long.valueOf(KafkaChannel.this.pollTimeout), KafkaChannel.this.getName());
			try {
				this.records = this.consumer.poll(KafkaChannel.this.pollTimeout);
				this.recordIterator = this.records.iterator();
				KafkaChannel.logger.debug("{} returned {} records from last poll", KafkaChannel.this.getName(),
						Integer.valueOf(this.records.count()));
			} catch (WakeupException e) {
				KafkaChannel.logger.trace("Consumer woken up for channel {}.", KafkaChannel.this.getName());
			}
		}

		private void commitOffsets() {
			try {
				this.consumer.commitSync(this.offsets);
			} catch (Exception e) {
				KafkaChannel.logger.info("Error committing offsets.", e);
			} finally {
				KafkaChannel.logger.trace("About to clear offsets map.");
				this.offsets.clear();
			}
		}

		private String getOffsetMapString() {
			StringBuilder sb = new StringBuilder();
			sb.append(KafkaChannel.this.getName()).append(" current offsets map: ");
			for (TopicPartition tp : this.offsets.keySet()) {
				sb.append("p").append(tp.partition()).append("-").append(this.offsets.get(tp).offset()).append(" ");
			}
			return sb.toString();
		}

		private String getCommittedOffsetsString() {
			StringBuilder sb = new StringBuilder();
			sb.append(KafkaChannel.this.getName()).append(" committed: ");
			for (TopicPartition tp : this.consumer.assignment()) {
				try {
					sb.append("[").append(tp).append(",").append(this.consumer.committed(tp).offset()).append("] ");
				} catch (NullPointerException npe) {
					KafkaChannel.logger.debug("Committed {}", tp);
				}
			}
			return sb.toString();
		}

		private void saveOffsets(TopicPartition tp, OffsetAndMetadata oam) {
			this.offsets.put(tp, oam);
			if (KafkaChannel.logger.isTraceEnabled()) {
				KafkaChannel.logger.trace(this.getOffsetMapString());
			}
		}
	}
}
