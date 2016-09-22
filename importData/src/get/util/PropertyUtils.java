package get.util;

import java.util.Properties;

import org.apache.log4j.Logger;

public class PropertyUtils {
	
	private static Logger logger= Logger.getLogger(PropertyUtils.class);
	
	public static String getBasicConfig(String key){
		try {
			Properties properties = new Properties();
//			String paht = System.getProperty("user.dir") + "/config/server.properties";
//			logger.info(paht);
			properties.load(PropertyUtils.class.getClassLoader().getResourceAsStream("server.properties"));
			return properties.getProperty(key);
		} catch (Exception e) {
			logger.error("server.properties文件异常" + e.getMessage());
		}
		return "";
	}
}
