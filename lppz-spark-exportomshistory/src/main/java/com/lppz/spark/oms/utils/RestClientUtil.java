package com.lppz.spark.oms.utils;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJacksonHttpMessageConverter;
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

public class RestClientUtil {
	private static final Logger logger = LoggerFactory.getLogger(RestClientUtil.class);
	private RestTemplate restTemplate;
	private String baseUrl;
	
//	public RestClientUtil(String baseUrl){
//		SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
//        requestFactory.setConnectTimeout(10000);
//        requestFactory.setReadTimeout(10000);
//        this.baseUrl = baseUrl;
//
//        restTemplate = new RestTemplate(requestFactory);
//	}
	public RestClientUtil(String baseUrl){
		this.baseUrl = baseUrl;
		
	}
	
	@SuppressWarnings("unchecked")
	public AreaPo getAreaByCode(String code){
		AreaPo area = null;
		String url = baseUrl + "/getAreaJson";
		CacheParam param = new CacheParam(code, AreaEnum.idxCode);
        String responseResult = doPost(url, param);
		Map<String,JSONObject> areaMap = JSON.parseObject(responseResult, Map.class );
        if (areaMap != null && !areaMap.isEmpty()&&areaMap.get(code) != null) {
        	area = areaMap.get(code).toJavaObject(AreaPo.class);
		}
		return area;
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
	
	
	private String doPost(String url,CacheParam param){
		try {
			HttpHeaders headers = new HttpHeaders();
			MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
			headers.setContentType(type);
			
			HttpEntity<CacheParam> requestEntity = new HttpEntity<CacheParam>(param,  headers);
			
			SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
			requestFactory.setConnectTimeout(10000);
			requestFactory.setReadTimeout(10000);
			restTemplate = new RestTemplate(requestFactory);
			restTemplate.getMessageConverters().add(new MappingJacksonHttpMessageConverter());
			restTemplate.getMessageConverters().add(new StringHttpMessageConverter());
			return  restTemplate.postForObject(url, requestEntity, String.class);
		} catch (Exception e) {
			logger.error("rest do post exception url:" + url + " params : " + param,e);
		}
		return null;
	}

}
