package com.lppz.spark;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lppz.util.curator.CuratorFrameworkUtils;
import com.lppz.util.curator.enums.ZookeeperPath;

/**
 * 
 * @author licheng
 *
 */
public class LockHandler {
	private static final Logger logger = LoggerFactory.getLogger(LockHandler.class);
	private CuratorFramework curator;
	public static final String parent = "lock";
	public static final String node = "dataSync";
	private String lockPath;
	
	public LockHandler(String zkAddress) {
		lockPath = ZookeeperPath.ZK_SEPARATOR.getKey() + parent + ZookeeperPath.ZK_SEPARATOR.getKey() + node;
		this.curator = CuratorFrameworkUtils.buildConnection(zkAddress);
	}
	
	public boolean lockOn(){
		String value = "1";
		try {
			String  result = CuratorFrameworkUtils.createTempNodeWithoutRetry(parent, node, value, curator);
			if (result != null) {
				return true;
			}
		} catch (Exception e) {
			logger.error("lock on fail",e);
		}
		return false;
	}
	
	public boolean lockOff(){
		
		try {
			CuratorFrameworkUtils.delTempNode(lockPath, curator);
			return true;
		} catch (Exception e) {
			logger.error("lock off fail", e);
		}
		return false;
	}
	
	public boolean isLock(){
		try {
			return CuratorFrameworkUtils.isExist(curator, lockPath);
		} catch (Exception e) {
			logger.error("get lock stauts fail", e);
		}
		return false;
	}

	public void close(){
		curator.close();
	}
}
