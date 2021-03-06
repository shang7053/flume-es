package com.scc.flume.source.rabbit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractPollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * @ClassName: KafkaSource
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author shangchengcai@voole.com
 * @date 2017年12月18日 上午9:40:46
 * 
 */
public class RabbitSource extends AbstractPollableSource implements Configurable {
	private static final Logger LOGGER = LoggerFactory.getLogger(RabbitSource.class);
	private static final ConcurrentLinkedQueue QUEUE = new ConcurrentLinkedQueue();
	private List<Channel> constomerChannels = new ArrayList<>();
	private ConnectionFactory factory;
	private Connection connection;
	private String exchangename;
	private String queuename;
	private String routingkey;
	private String exchangetype;
	private Integer capacity;
	private Integer maxtmpsize;
	private Long delaytime;
	private Integer customers;

	/*
	 * (非 Javadoc) <p>Title: doProcess</p> <p>Description: </p>
	 * 
	 * @return
	 * 
	 * @throws EventDeliveryException
	 * 
	 * @see org.apache.flume.source.AbstractPollableSource#doProcess()
	 */
	@Override
	protected Status doProcess() throws EventDeliveryException {
		LOGGER.debug("-------------------------rabbit source get data start-------------------------");
		try {
			List<Map<String, Object>> datas = new ArrayList<>();
			for (int i = 0; i < RabbitSource.this.capacity; i++) {
				byte[] body = (byte[]) QUEUE.poll();
				if (null == body) {
					LOGGER.debug("no new message,will be break! current enevt size is {}", datas.size());
					break;
				}
				datas.add(this.deserializeValue(body));
			}
			if (datas.size() > 0) {
				this.getChannelProcessor().processEvent(EventBuilder.withBody(this.serializeValue(datas), null));
			} else {
				return Status.READY;
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return Status.BACKOFF;
		}
		LOGGER.debug("-------------------------rabbit source get data end-------------------------");
		return Status.READY;
	}

	/*
	 * (非 Javadoc) <p>Title: doConfigure</p> <p>Description: </p>
	 * 
	 * @param context
	 * 
	 * @throws FlumeException
	 * 
	 * @see org.apache.flume.source.BasicSourceSemantics#doConfigure(org.apache.flume.Context)
	 */
	@Override
	protected void doConfigure(Context context) throws FlumeException {
		LOGGER.info("=========================start config rabbit source==============================");
		try {
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
			this.customers = context.getInteger("customers", 1);
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
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			e.printStackTrace();
			System.exit(1);
		}
		LOGGER.info("=========================end config rabbit source==============================");
	}

	/*
	 * (非 Javadoc) <p>Title: doStart</p> <p>Description: </p>
	 * 
	 * @throws FlumeException
	 * 
	 * @see org.apache.flume.source.BasicSourceSemantics#doStart()
	 */
	@Override
	protected void doStart() throws FlumeException {
		LOGGER.info("-------------------do start rabbit source--------------------");
		for (int i = 0; i < this.customers; i++) {
			try {
				Channel customerChannel = this.connection.createChannel();
				this.constomerChannels.add(customerChannel);
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
						if (QUEUE.size() > RabbitSource.this.maxtmpsize) {
							LOGGER.info("too many message!sleep for {}ms", RabbitSource.this.delaytime);
							try {
								Thread.sleep(RabbitSource.this.delaytime);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
					}
				});
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		LOGGER.info("-------------------end do start rabbit source--------------------");
	}

	/*
	 * (非 Javadoc) <p>Title: doStop</p> <p>Description: </p>
	 * 
	 * @throws FlumeException
	 * 
	 * @see org.apache.flume.source.BasicSourceSemantics#doStop()
	 */
	@Override
	protected void doStop() throws FlumeException {
		LOGGER.info("-------------------start stop rabbit source--------------------");
		try {
			for (Channel channel : this.constomerChannels) {
				channel.close();
			}
			this.connection.close();
			while (QUEUE.size() > 0) {
				Thread.sleep(1000l);
			}
		} catch (IOException | InterruptedException | TimeoutException e) {
			e.printStackTrace();
		}
		LOGGER.info("-------------------end stop rabbit source--------------------");
	}

	private Map<String, Object> deserializeValue(byte[] value) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bin = new ByteArrayInputStream(value);
		ObjectInputStream obin = new ObjectInputStream(bin);
		Map<String, Object> event = (Map<String, Object>) obin.readObject();
		return event;
	}

	private byte[] serializeValue(List<Map<String, Object>> datas) throws IOException {
		ByteArrayOutputStream obj = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(obj);
		out.writeObject(datas);
		return obj.toByteArray();
	}

}
