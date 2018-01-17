package com.lppz.spark.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJacksonHttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lppz.oms.api.model.hbase.HbaseOrderBean;

public class HbaseHandlerHttp implements Serializable{
	private static RestTemplate restTemplate;
	
	private static final long serialVersionUID = 1L;
	
	private static final String baseUrl = "http://127.0.0.1:9999/services/histOrder";

	private static final Logger LOG = Logger.getLogger(HbaseHandlerHttp.class);

	private File file;
	
	public HbaseHandlerHttp(String filePath) {
		SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(10000);
        requestFactory.setReadTimeout(10000);
        
        restTemplate = new RestTemplate(requestFactory);
        restTemplate.getMessageConverters().add(new MappingJacksonHttpMessageConverter());
		restTemplate.getMessageConverters().add(new StringHttpMessageConverter());
		file = new File(filePath);
		file.delete();
	}
	
	public static void main(String[] args) {
		System.out.println("hello world");
		List<String> orderIds = new ArrayList<String>();
		orderIds.add("MG171013047380271");
		orderIds.add("WX171013047380272");
		
		HbaseHandlerHttp handler = new HbaseHandlerHttp("/opt/xxx.txt");
		for(int i=0;i<100;i++){
			Map<String,TreeSet<HbaseOrderBean>> result = handler.scanResultByRowKey(orderIds);
		}
		
	}
	
	public void handle(List<String> orderIds) {
		Map<String, TreeSet<HbaseOrderBean>> result = scanResultByRowKey(orderIds);
		if(result == null || result.size() <= 0) {
			return;
		}
		
		for(TreeSet<HbaseOrderBean> set: result.values()) {
			if(set.size() > 1) {
				
				set.pollFirst();
				for (HbaseOrderBean hob : set) {
					LOG.info(String.format("spark: delete data rowkey: %s", hob.getRowKey()));
					/*try {
						hbi.deleteAllColumn("hbaseorder", "order", hob.getRowKey());
					} catch (IOException e) {
						LOG.error(String.format("hbaseorder delete fail, rowkey: %s", hob.getRowKey()));
					}*/
				}
				writeFile(set);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, TreeSet<HbaseOrderBean>> scanResultByRowKey(List<String> orderIds) {
		Map<String, TreeSet<HbaseOrderBean>> result = new HashMap<String, TreeSet<HbaseOrderBean>>();
		
		String url = baseUrl + "/scanHbaseRowKey";
		String responseResult = doPost(url, orderIds);
		Map<String, List<JSONObject>> temp = JSON.parseObject(responseResult, Map.class);
		for(String key : temp.keySet()) {
			TreeSet<HbaseOrderBean> list = new TreeSet<HbaseOrderBean>(new ComparatorOrder());
			for(JSONObject jo: temp.get(key)) {
				list.add(jo.toJavaObject(HbaseOrderBean.class));
			}
			result.put(key, list);
		}
		return result;
	}
	
	@SuppressWarnings("rawtypes")
	private String doPost(String url ,List<String> orderIds){
		HttpHeaders headers = new HttpHeaders();
		MediaType type = MediaType.parseMediaType("application/json");
		headers.setContentType(type);
		
		HttpEntity<List> requestEntity = new HttpEntity<List>(orderIds,  headers);
		
		return restTemplate.postForObject(url, requestEntity, String.class);
	}
	
	private void writeFile(TreeSet<HbaseOrderBean> hobs) {
		FileOutputStream fos = null;
		try{
			fos = new FileOutputStream(file, true);
			for(HbaseOrderBean hob: hobs) {
				fos.write((hob.getRowKey()+"\r\n").getBytes());
			}
		}catch(Exception e) {
			LOG.error("write file error.", e);
		}finally {
			if(fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					LOG.error("FileOutputStream close error.", e);
				}
			}
		}
	}
	
}