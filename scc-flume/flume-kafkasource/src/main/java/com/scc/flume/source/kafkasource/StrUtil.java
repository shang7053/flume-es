package com.scc.flume.source.kafkasource;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName: StrUtil
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author shangchengcai@voole.com
 * @date 2017年12月18日 下午3:34:54
 * 
 */
public class StrUtil {
	/**
	 * 
	 * @Title: getMatcherStrs
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @author shangchengcai@voole.com
	 * @date 2017年12月18日 下午3:44:26
	 * @param content
	 * @param sPattern
	 * @return 获取第一个日期，如果不存在就拿当前时间
	 */
	public static Date getFirstDate(String str) {
		Pattern p = Pattern.compile(
				"[1-9]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])\\s+(20|21|22|23|[0-1]\\d):[0-5]\\d:[0-5]\\d");
		Matcher m = p.matcher(str);
		if (m.find()) {
			try {
				return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(m.group());
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return new Date();
	}

	/**
	 * 
	 * @Title: getFirstIp
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @author shangchengcai@voole.com
	 * @date 2017年12月18日 下午3:49:53
	 * @param str
	 * @return 获取第一个ip，如果不存在就是127.0.0.1
	 */
	public static String getFirstIp(String str) {
		Pattern p = Pattern.compile(
				"((25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))");
		Matcher m = p.matcher(str);
		if (m.find()) {
			return m.group();
		}
		return "127.0.0.1";
	}

	public static void main(String[] args) {
		System.out.println(getFirstDate("2014-11-11 11:23:45"));
		System.out.println(getFirstDate("my birthday is 2014-11-11 11:23:45"));
		System.out.println(getFirstIp("111.111.111.111"));
		System.out.println(getFirstIp("111.111.333.111"));
		System.out.println(getFirstIp("111.111.111"));
		System.out.println(getLevel("asdasdasdasd INFO asdasd"));
	}

	/**
	 * @Title: getLevel
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @author shangchengcai@voole.com
	 * @date 2017年12月19日 下午5:50:09
	 * @param msg
	 * @return
	 */
	public static String getLevel(String str) {
		Pattern p = Pattern.compile("INFO");
		Matcher m = p.matcher(str);
		if (m.find()) {
			return m.group();
		}
		p = Pattern.compile("WARN");
		m = p.matcher(str);
		if (m.find()) {
			return m.group();
		}
		p = Pattern.compile("ERROR");
		m = p.matcher(str);
		if (m.find()) {
			return m.group();
		}
		p = Pattern.compile("FATAL");
		m = p.matcher(str);
		if (m.find()) {
			return m.group();
		}
		p = Pattern.compile("DEBUG");
		m = p.matcher(str);
		if (m.find()) {
			return m.group();
		}
		p = Pattern.compile("ALL");
		m = p.matcher(str);
		if (m.find()) {
			return m.group();
		}
		return "INFO";
	}
}
