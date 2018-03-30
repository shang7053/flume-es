package com.scc.flume.channel.rabbitchannel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

/**
 * @ClassName: rabbitChannel2
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author shangchengcai@voole.com
 * @date 2018年1月19日 下午2:27:43
 * 
 */
public class RabbitChannel extends BasicChannelSemantics {
	private static final Logger LOGGER = LoggerFactory.getLogger(RabbitChannel.class);
	private static final ConcurrentLinkedQueue QUEUE = new ConcurrentLinkedQueue();
	private ConnectionFactory factory;
	private Connection connection;
	private Channel producerChannel;
	private String exchangename;
	private String queuename;
	private String routingkey;
	private String exchangetype;
	private Integer capacity;
	private Integer maxtmpsize;
	private Long delaytime;
	private Integer customers;

	/*
	 * (非 Javadoc) <p>Title: createTransaction</p> <p>Description: </p>
	 * 
	 * @return
	 * 
	 * @see org.apache.flume.channel.BasicChannelSemantics#createTransaction()
	 */
	@Override
	protected BasicTransactionSemantics createTransaction() {
		return new RabbitTransaction();
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
		LOGGER.info("=========================start config rabbit channel==============================");
		try {
			super.configure(context);
			String address = context.getString("address");
			String uname = context.getString("uname");
			String pwd = context.getString("pwd");
			String virtualHost = context.getString("virtualHost", "/");
			this.exchangename = context.getString("exchange.name");
			this.exchangetype = context.getString("exchange.type", "direct");
			this.queuename = context.getString("queuename");
			this.routingkey = context.getString("routingkey");
			this.capacity = context.getInteger("capacity", 1);
			this.maxtmpsize = context.getInteger("maxtmpsize", 100000);
			this.delaytime = context.getLong("delaytime", 1000L);
			this.customers = context.getInteger("customers", 2);
			if ((address == null) || (address.isEmpty())) {
				throw new ConfigurationException("address must be specified");
			}
			if ((uname == null) || (uname.isEmpty())) {
				throw new ConfigurationException("uname must be specified");
			}
			if ((pwd == null) || (pwd.isEmpty())) {
				throw new ConfigurationException("pwd must be specified");
			}
			if ((this.exchangename == null) || (this.exchangename.isEmpty())) {
				throw new ConfigurationException("exchangename must be specified");
			}
			if ((this.exchangetype == null) || (this.exchangetype.isEmpty())) {
				throw new ConfigurationException("exchangetype must be specified");
			}
			if ((this.queuename == null) || (this.queuename.isEmpty())) {
				this.queuename = this.exchangename;
				this.routingkey = this.exchangename;
			}
			if ((this.routingkey == null) || (this.routingkey.isEmpty())) {
				this.routingkey = this.exchangename;
			}
			this.exchangename = "voole.datasync." + this.exchangename;
			LOGGER.info("get config address={}", address);
			LOGGER.info("get config uname={}", uname);
			LOGGER.info("get config pwd={}", pwd);
			LOGGER.info("get config virtualHost={}", virtualHost);
			LOGGER.info("get config exchangename={}", this.exchangename);
			LOGGER.info("get config exchangetype={}", this.exchangetype);
			LOGGER.info("get config queuename={}", this.queuename);
			LOGGER.info("get config routingkey={}", this.queuename);
			LOGGER.info("get config capacity={}", this.capacity);
			LOGGER.info("get config maxtmpsize={}", this.maxtmpsize);
			LOGGER.info("get config customers={}", this.customers);
			List<Address> addresses = new ArrayList<>();
			String[] strAddresses = address.split(",");
			for (String strAddress : strAddresses) {
				if ((strAddress != null) && (!strAddress.isEmpty())) {
					addresses.add(new Address(strAddress.split(":")[0], Integer.valueOf(strAddress.split(":")[1])));
				}
			}
			LOGGER.info("create rabbit ConnectionFactory……");
			this.factory = new ConnectionFactory();
			this.factory.setAutomaticRecoveryEnabled(true);
			this.factory.setTopologyRecoveryEnabled(true);
			this.factory.setUsername(uname);
			this.factory.setPassword(pwd);
			this.factory.setVirtualHost(virtualHost);
			LOGGER.info("create rabbit Connection……");
			this.connection = this.factory.newConnection(addresses.toArray(new Address[addresses.size()]));
			this.producerChannel = this.connection.createChannel();
			this.producerChannel.exchangeDeclare(this.exchangename, this.exchangetype, true, false, null);
			this.producerChannel.queueDeclare(this.queuename, true, false, false, null);
			this.producerChannel.queueBind(this.queuename, this.exchangename, this.routingkey);

			for (int i = 0; i < this.customers; i++) {
				Channel customerChannel = this.connection.createChannel();
				// 声明一个交换器
				customerChannel.exchangeDeclare(this.exchangename, this.exchangetype, true, false, null);
				// 声明一个持久化的队列
				customerChannel.queueDeclare(this.exchangename, true, false, false, null);
				// 绑定队列，通过键 hola 将队列和交换器绑定起来
				customerChannel.queueBind(this.queuename, this.exchangename, this.routingkey);
				customerChannel.basicConsume(this.queuename, true, new DefaultConsumer(customerChannel) {

					/*
					 * (非 Javadoc) <p>Title: handleDelivery</p> <p>Description: </p>
					 * 
					 * @param consumerTag
					 * 
					 * @param envelope
					 * 
					 * @param properties
					 * 
					 * @param body
					 * 
					 * @throws IOException
					 * 
					 * @see com.rabbitmq.client.DefaultConsumer#handleDelivery(java.lang.String,
					 * com.rabbitmq.client.Envelope, com.rabbitmq.client.AMQP.BasicProperties, byte[])
					 */
					@Override
					public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
							byte[] body) throws IOException {
						LOGGER.debug("channel message MessageId={}", properties.getMessageId());
						LOGGER.debug("channel message RoutingKey={}", envelope.getRoutingKey());
						LOGGER.debug("channel message ContentType={}", properties.getContentType());
						QUEUE.add(body);
						LOGGER.info("queue size ={}", QUEUE.size());
						if (QUEUE.size() > RabbitChannel.this.maxtmpsize) {
							LOGGER.info("too many message!sleep for {}ms", RabbitChannel.this.delaytime);
							try {
								Thread.sleep(RabbitChannel.this.delaytime);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
					}
				});
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			e.printStackTrace();
			System.exit(1);
		}
		LOGGER.info("=========================end config rabbit channel==============================");
	}

	/*
	 * (非 Javadoc) <p>Title: stop</p> <p>Description: </p>
	 * 
	 * @see org.apache.flume.channel.AbstractChannel#stop()
	 */
	@Override
	public synchronized void stop() {
		super.stop();
		try {
			this.producerChannel.close();
			this.connection.close();
			while (QUEUE.size() > 0) {
				Thread.sleep(1000l);
			}
		} catch (IOException | TimeoutException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	private class RabbitTransaction extends BasicTransactionSemantics {
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
			LOGGER.debug("-------------------------put channel data to rabbit start-------------------------");
			try {
				byte[] data = this.serializeValue(event);
				RabbitChannel.this.producerChannel.basicPublish(RabbitChannel.this.exchangename,
						RabbitChannel.this.routingkey, MessageProperties.PERSISTENT_TEXT_PLAIN, data);
				LOGGER.info("put event to channel success!data legth={}B", data.length);
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
				e.printStackTrace();
			}
			LOGGER.debug("-------------------------put channel data to rabbit end-------------------------");
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
				for (int i = 0; i < RabbitChannel.this.capacity; i++) {
					byte[] body = (byte[]) QUEUE.poll();
					if (null == body) {
						LOGGER.debug("no new message,will be break! current enevt size is {}", datas.size());
						break;
					}
					datas.add(RabbitTransaction.this.deserializeValue(body));
				}
				if (datas.size() > 0) {
					return EventBuilder.withBody(this.serializeValue(datas), null);
				}
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
				e.printStackTrace();
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
			// 确认消息
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
			// 无需事物
		}

		private byte[] serializeValue(List<Map<String, Object>> datas) throws IOException {
			ByteArrayOutputStream obj = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(obj);
			out.writeObject(datas);
			return obj.toByteArray();
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

		private Map<String, Object> deserializeValue(byte[] value) throws IOException, ClassNotFoundException {
			ByteArrayInputStream bin = new ByteArrayInputStream(value);
			ObjectInputStream obin = new ObjectInputStream(bin);
			Map<String, Object> event = (Map<String, Object>) obin.readObject();
			return event;
		}
	}

}
