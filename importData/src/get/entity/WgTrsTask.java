package get.entity;

/**
 * 
 * @author Gnahznib
 * 
 */
public class WgTrsTask {

	// 消息标识格式：YYMMDDhhmmssnnnn与请求参数一致
	private String sid;

	// 数据源编号(任务ID)
	private String dataSourceCode;

	// 数据源名称(任务名称)
	private String dataSourceName;

	// 数据源类别
	private int dataSourceType;

	// IP
	private String ip;

	// 端口号
	private int port;

	// 数据库名称
	private String dbName;

	// 数据库登录用户名
	private String username;

	// 数据库登录密码
	private String password;

	
	// 数据标识唯一值
	private int recordID;

	public int getRecordID() {
		return recordID;
	}

	public void setRecordID(int recordID) {
		this.recordID = recordID;
	}


	public String getSid() {
		return sid;
	}

	public void setSid(String sid) {
		this.sid = sid;
	}

	public String getDataSourceCode() {
		return dataSourceCode;
	}

	public void setDataSourceCode(String dataSourceCode) {
		this.dataSourceCode = dataSourceCode;
	}

	public String getDataSourceName() {
		return dataSourceName;
	}

	public void setDataSourceName(String dataSourceName) {
		this.dataSourceName = dataSourceName;
	}

	public int getDataSourceType() {
		return dataSourceType;
	}

	public void setDataSourceType(int dataSourceType) {
		this.dataSourceType = dataSourceType;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

}
