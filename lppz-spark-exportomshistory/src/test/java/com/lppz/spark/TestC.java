package com.lppz.spark;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJacksonHttpMessageConverter;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.RestTemplate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lppz.dubbo.oms.init.CacheParam;
import com.lppz.dubbo.oms.init.enums.AreaEnum;
import com.lppz.dubbo.oms.init.enums.BaseStoreEnum;
import com.lppz.dubbo.oms.init.enums.ProductEnum;
import com.lppz.dubbo.oms.init.enums.StockRoomLocationEnum;
import com.lppz.oms.api.entities.AreaPo;
import com.lppz.oms.api.entities.BaseStorePo;
import com.lppz.oms.api.entities.ProductPo;
import com.lppz.oms.api.entities.StockroomLocationPo;
import com.lppz.spark.oms.utils.RestClientUtil;


public class TestC {
	
	private static RestTemplate restTemplate;
	static String baseUrl = "http://192.168.19.184:11060/services/omscache";
	
	public static void main(String[] args) {
		try {
			RestClientUtil client = new RestClientUtil(baseUrl);
			System.out.println(client.getBaseStoreById("single|BaseStoreData|1002"));
			System.out.println(client.getBaseStoreById("single|BaseStoreData|1002"));
			System.out.println(client.getBaseStoreById("single|BaseStoreData|1002"));
			System.out.println(client.getStockRoomLocationById("single|StockroomLocationData|01"));
			System.out.println(client.getStockRoomLocationById("single|StockroomLocationData|01"));
			System.out.println(client.getStockRoomLocationById("single|StockroomLocationData|01"));
			System.out.println(client.getStockRoomLocationById("single|StockroomLocationData|01"));
			System.out.println(client.getAreaByCode("420000"));
			System.out.println(client.getProductByProductId("11000013"));
			System.out.println(client.getBaseStoreById("single|BaseStoreData|1002"));
			System.out.println(client.getStockRoomLocationById("single|StockroomLocationData|01"));
			System.out.println(client.getAreaByCode("150110"));
			System.out.println(client.getProductByProductId("11000013"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void initthis(){
		SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(10000);
        requestFactory.setReadTimeout(10000);
        
        restTemplate = new RestTemplate(requestFactory);
	}
	
	public void testMutilRest(){
		testAreaGetByCode();
		System.out.println(getProductByProductId("11000013"));
		System.out.println(getStockRoomLocationById("single|StockroomLocationData|01"));
		System.out.println(getBaseStoreById("single|BaseStoreData|1002"));
	}
	
	@SuppressWarnings("unchecked")
	public void testAreaGetByCode(){
		String code = "420000";
		String url = baseUrl + "/getAreaJson";
		AreaPo area = null;
		CacheParam param = new CacheParam(code, AreaEnum.idxCode);
		String responseResult = doPost(url, param);
        Map<String,JSONObject> areaMap = JSON.parseObject(responseResult, Map.class );
        if (areaMap != null && !areaMap.isEmpty()) {
        	area = areaMap.get(code).toJavaObject(AreaPo.class);
		}
        
        System.out.println(area);
	}
	
	public ProductPo getProductByProductId(String productId){
		ProductPo product = null;
		String url = baseUrl + "/getProductJson";
		CacheParam param = new CacheParam(productId, ProductEnum.idxProductId);
        String responseResult = doPost(url, param);
        if (StringUtils.isNotBlank(responseResult)) {
        	product = JSON.parseObject(responseResult, ProductPo.class);
		}
		return product;
	}
	
	public StockroomLocationPo getStockRoomLocationById(String lacationId){
		StockroomLocationPo location = null;
		String url = baseUrl + "/getStockRoomLocationJson";
		CacheParam param = new CacheParam(lacationId, StockRoomLocationEnum.idxLocationId);
		String responseResult = doPost(url, param);
		if (StringUtils.isNotBlank(responseResult)) {
			location = JSON.parseObject(responseResult, StockroomLocationPo.class);
		}
		return location;
	}
	
	public BaseStorePo getBaseStoreById(String storeid){
		BaseStorePo store = null;
		String url = baseUrl + "/getBaseStoreJson";
		CacheParam param = new CacheParam(storeid, BaseStoreEnum.idxId);
		String responseResult = doPost(url, param);
		if (StringUtils.isNotBlank(responseResult)) {
			store = JSON.parseObject(responseResult, BaseStorePo.class);
		}
		return store;
	}
	
	private String doPost(String url ,CacheParam param){
		HttpHeaders headers = new HttpHeaders();
		MediaType type = MediaType.parseMediaType("application/json");
		headers.setContentType(type);
		
		HttpEntity<CacheParam> requestEntity = new HttpEntity<CacheParam>(param,  headers);
		restTemplate.getMessageConverters().add(new MappingJacksonHttpMessageConverter());
		restTemplate.getMessageConverters().add(new StringHttpMessageConverter());
		return restTemplate.postForObject(url, requestEntity, String.class);
	}

}
