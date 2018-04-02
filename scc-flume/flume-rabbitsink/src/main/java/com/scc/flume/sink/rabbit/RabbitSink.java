package com.scc.flume.sink.rabbit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

/**
 * @ClassName: EsSink
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author shangchengcai@voole.com
 * @date 2017年12月18日 下午4:23:07
 * 
 */
public class RabbitSink extends AbstractSink implements Configurable {
	private static final Logger LOGGER = LoggerFactory.getLogger(RabbitSink.class);
	private ConnectionFactory factory;
	private Connection connection;
	private Channel producerChannel;
	private String exchangename;
	private String queuename;
	private String routingkey;
	private String exchangetype;

	/*
	 * (非 Javadoc) <p>Title: process</p> <p>Description: </p>
	 * 
	 * @return
	 * 
	 * @throws EventDeliveryException
	 * 
	 * @see org.apache.flume.Sink#process()
	 */
	@Override
	public Status process() throws EventDeliveryException {
		LOGGER.debug("-------------------------put channel data to rabbit start-------------------------");
		org.apache.flume.Channel channel = this.getChannel();
		Transaction transaction = channel.getTransaction();
		try {
			transaction.begin();
			Event event = channel.take();
			if (event == null) {
				LOGGER.debug("no event recive……");
				transaction.commit();
				return Status.READY;
			}
			byte[] data = this.serializeValue(event);
			this.producerChannel.basicPublish(this.exchangename, this.routingkey,
					MessageProperties.PERSISTENT_TEXT_PLAIN, data);
			LOGGER.info("put event to rabbit success!data legth={}B", data.length);
			transaction.commit();
		} catch (Exception e) {
			transaction.rollback();
			LOGGER.error("process rabbit sink happended a error!{}", e);
			return Status.BACKOFF;
		} finally {
			transaction.close();
		}
		LOGGER.debug("-------------------put channel data to rabbit start end--------------------");
		return Status.READY;
	}

	/*
	 * (非 Javadoc) <p>Title: configure</p> <p>Description: </p>
	 * 
	 * @param context
	 * 
	 * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	@Override
	public void configure(Context context) {
		LOGGER.info("=========================start config rabbit sink==============================");
		try {
			String address = context.getString("address");
			String uname = context.getString("uname");
			String pwd = context.getString("pwd");
			String virtualHost = context.getString("virtualHost", "/");
			this.exchangename = context.getString("exchange.name");
			this.exchangetype = context.getString("exchange.type", "direct");
			this.queuename = context.getString("queuename");
			this.routingkey = context.getString("routingkey");
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
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		LOGGER.info("=========================end config rabbit sink==============================");
	}

	/*
	 * (非 Javadoc) <p>Title: stop</p> <p>Description: </p>
	 * 
	 * @see org.apache.flume.sink.AbstractSink#stop()
	 */
	@Override
	public synchronized void stop() {
		LOGGER.info("-------------------do stop rabbit sink start--------------------");
		super.stop();
		try {
			this.connection.close();
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
		}
		LOGGER.info("-------------------do stop rabbit sink end--------------------");
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

}
