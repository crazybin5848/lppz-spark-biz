package com.lppz.spark.accmember;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

public class ZkUtil {
	
	private CuratorFramework zkConn=null;
	
	private static ZkUtil instance=new ZkUtil();
	
	public static ZkUtil getInstance(){
		return instance;
	}
	
	private ZkUtil(){
		this.zkConn=buildConnection("10.8.102.205:2181");
	}
	
	public void close(){
		if(null!=zkConn){
			zkConn.close();
		}
	}

	public static void main(String[] args) throws Exception{
		ZkUtil zk=new ZkUtil();
		CuratorFramework zkConn = buildConnection("10.8.102.205:2181");
		
		if(zk.checkPathExists("/mycat/mycat-cluster-member-prod/line")){
			List<String> children=zk.getChildNames(zkConn,"/mycat/mycat-cluster-member-prod/line");
			
			if(null!=children && children.size()>0){
				for(String ch:children){
					String data=zk.getDataToString(zkConn, "/mycat/mycat-cluster-member-prod/line".concat("/").concat(ch));
					System.out.println(data);
				}
			}
		}
		
		zkConn.close();
	}
	public boolean checkPathExists(String path) {
        try {
            Stat state = this.zkConn.checkExists().forPath(path);

            if (null != state) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
	
	public List<String> getChildNames(CuratorFramework zkConn,String path) throws Exception {
        return zkConn.getChildren().forPath(path);
    }
	
	public String getDataToString(CuratorFramework curator,String path) throws Exception {
        byte[] raw = curator.getData().forPath(path);

        return byteToString(raw);
    }
	private String byteToString(byte[] raw) {
        // return empty json {}.
        if (raw.length == 0) {
            return "{}";
        }
        return new String(raw, StandardCharsets.UTF_8);
    }

	private static CuratorFramework buildConnection(String url) {
		CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(
				url, new ExponentialBackoffRetry(100, 6));

		// start connection
		curatorFramework.start();
		// wait 3 second to establish connect
		try {
			curatorFramework.blockUntilConnected(3, TimeUnit.SECONDS);
			if (curatorFramework.getZookeeperClient().isConnected()) {
				return curatorFramework.usingNamespace("");
			}
		} catch (InterruptedException ignored) {
			Thread.currentThread().interrupt();
		}

		// fail situation
		curatorFramework.close();
		throw new RuntimeException("failed to connect to zookeeper service : "
				+ url);
	}
}
