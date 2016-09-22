package get.util;

import java.net.InetSocketAddress;
import java.util.concurrent.Future;

import net.spy.memcached.MemcachedClient;

import org.apache.log4j.Logger;

public class MemcachedJava {
	
	// 收集日志
	private static Logger logger = Logger.getLogger(MemcachedJava.class);
	private static MemcachedClient mcc = null;
	
	/**
	 * @Description:为调用MC客户端者提供连接客户端方法 
	 * @Author: ZhangXingBin
	 */
	public void connection(){
		// 连接Memcached服务
		String ip = PropertyUtils.getBasicConfig("McAddr");
		int port = Integer.valueOf(PropertyUtils.getBasicConfig("McPort"));
		
		try{
			// 连接MC客户端
			mcc = new MemcachedClient(new InetSocketAddress(ip, port));
			logger.info("Connection to server sucessful.");
		}catch(Exception e){
			logger.error("Connection to server failure.");
		}
	}
	
	/**
	 * @Description: 连接 Memcached服务，并将数据发生至Memcached中
	 * @Author: ZhangXingBin
	 */
	public void Memcached(String value){
		// 将数据set到MC中
		Future<?> fo = mcc.set("reapout", 900, value);
		// 查看存储状态
		try {
			logger.info("MC set数据status:" + fo.get());
		} catch (Exception e) {
			logger.info(e.getMessage());
		} 
	}
	
	/**
	 * @Description:为调用MC客户端者提供关闭连接方法 
	 * @Author: ZhangXingBin
	 */
	public void loseMC(){
		// 关闭连接
		mcc.shutdown();
		logger.info("MC客户端关闭！");
	}
}
