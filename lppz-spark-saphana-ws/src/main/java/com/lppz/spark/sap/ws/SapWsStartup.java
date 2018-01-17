package com.lppz.spark.sap.ws;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.helpers.LogLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;


/**
 * @author binzou
 */
@Configuration
@PropertySource("classpath:/META-INF/dubbo.properties")
@ImportResource({"classpath:/META-INF/sap-ws-spring.xml"})
public class SapWsStartup {
	static Logger logger = LoggerFactory.getLogger(SapWsStartup.class);
	private static AnnotationConfigApplicationContext context;
	public volatile static boolean flag=true;
	static {
		context = new AnnotationConfigApplicationContext(SapWsStartup.class);
	}
	
	@Bean
	public static PropertyPlaceholderConfigurer placehodlerConfigurer() {
		PropertyPlaceholderConfigurer pc= new PropertyPlaceholderConfigurer();
		try {
			Resource resource = new ClassPathResource("/META-INF/dubbo.properties");
			pc.setLocation(resource);
			pc.setSystemPropertiesModeName("SYSTEM_PROPERTIES_MODE_OVERRIDE");
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		return pc;
	}
	
	public static void main(String[] args) {
		try {
			// startup
			context.start();
			logger.info("sap-ws server startup successfully.");
			while (flag) {
				Thread.sleep(10 * 1000);
			}
			context.destroy();
//			BaseKafkaConsumer.pool.awaitTermination(10, TimeUnit.SECONDS);
			System.exit(0);
		} catch (Exception e) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			LogLog.error(sdf.format(new Date()) + " startup error", e);
			context.stop();
			System.exit(-1);
		}
	}
}