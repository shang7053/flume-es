package com.scc.flume.channel.kafkachannel;

import java.io.Serializable;
import java.util.Map;

/**
 * @ClassName: LogData
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author shangchengcai@voole.com
 * @date 2018年1月19日 下午4:41:24
 * 
 */
public class LogData implements Serializable {
	/**
	 * @Fields serialVersionUID : TODO(用一句话描述这个变量表示什么)
	 * @author shangchengcai@voole.com
	 * @date 2018年1月19日 下午4:35:20
	 */
	private static final long serialVersionUID = 996413616131669504L;
	private Map<String, String> headers;
	private byte[] body;

	/**
	 * @return the headers
	 * @author shangchengcai@voole.com
	 * @date 2018年1月19日 下午4:35:09
	 */
	public Map<String, String> getHeaders() {
		return this.headers;
	}

	/**
	 * @author shangchengcai@voole.com
	 * @date 2018年1月19日 下午4:35:09
	 * @param headers the headers to set
	 */
	public void setHeaders(Map<String, String> headers) {
		this.headers = headers;
	}

	/**
	 * @return the body
	 * @author shangchengcai@voole.com
	 * @date 2018年1月19日 下午4:35:09
	 */
	public byte[] getBody() {
		return this.body;
	}

	/**
	 * @author shangchengcai@voole.com
	 * @date 2018年1月19日 下午4:35:09
	 * @param body the body to set
	 */
	public void setBody(byte[] body) {
		this.body = body;
	}

}
