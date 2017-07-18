package org.yujoo.baas.storm.demo;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class IPTest {

	
	public static void main(String[] args) {
		
		String httpUrl = "http://apis.map.qq.com/ws/location/v1/ip";
		String httpArg = "ip=210.21.221.18";
		String httpkey = "key=56GBZ-FDGWJ-KBYF5-FEX3L-FQECH-W6BCB";
		String jsonResult = request(httpUrl, httpArg,httpkey);
		System.out.println(jsonResult);

	}
	/**
	* @param urlAll
	* :请求接口
	* @param httpArg
	* :参数
	* @return 返回结果
	*/
	public static String request(String httpUrl, String httpArg,String httpkey) {
	BufferedReader reader = null;
	String result = null;
	StringBuffer sbf = new StringBuffer();
	httpUrl = httpUrl + "?" + httpArg+"&"+httpkey;

	try {
	URL url = new URL(httpUrl);
	HttpURLConnection connection = (HttpURLConnection) url
	.openConnection();
	connection.setRequestMethod("GET");
	// 填入apikey到HTTP header
	connection.setRequestProperty("key", "56GBZ-FDGWJ-KBYF5-FEX3L-FQECH-W6BCB");
	connection.connect();
	InputStream is = connection.getInputStream();
	reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
	String strRead = null;
	while ((strRead = reader.readLine()) != null) {
	sbf.append(strRead);
	sbf.append("\r\n");
	}
	reader.close();
	result = sbf.toString();
	} catch (Exception e) {
	e.printStackTrace();
	}
	return result;
	}

}
