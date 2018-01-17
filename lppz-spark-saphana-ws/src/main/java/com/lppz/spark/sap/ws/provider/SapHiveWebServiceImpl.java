package com.lppz.spark.sap.ws.provider;

import com.alibaba.dubbo.config.annotation.Service;
import com.lppz.spark.sap.ws.api.Sap2HiveWebService;

@Service(protocol="webservice",timeout=100000)
public class SapHiveWebServiceImpl implements Sap2HiveWebService {

	@Override
	public String sayHello(String name) {
		return "Hello "+name;
	}

	@Override
	public boolean notify(String params) {
		return true;
	}

}
