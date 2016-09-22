package get.link;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

import com.alibaba.fastjson.JSON;
import com.trs.client.TRSConnection;
import com.trs.client.TRSDataBase;
import com.trs.client.TRSDataBaseColumn;
import com.trs.client.TRSException;
import com.trs.client.TRSRecord;
import com.trs.client.TRSResultSet;
import com.trs.client.TRSView;

import get.entity.OilImageData;
import get.entity.TaskEndResult;
import get.entity.WgTrsTask;
import get.util.MemcachedJava;
import get.util.RedisUtil;
import get.util.TrsMD5;
import get.util.TrsUtil;

/**
 * @Package: readTrs.link
 * @Description: 读取TRS数据库数据
 * @Version: V0.1
 * @Author: ZhangXingBin
 * @ChangeHistoryList: Version     Author      		Date                    Description
 *                     V0.1        ZhangXingBin     2016年3月10日 下午4:48:46
 */

public class LinkTRS {

	private static Logger logger = Logger.getLogger(LinkTRS.class);

	/**
	 * @Description 读取数据库
	 * @param bean
	 * @return
	 * @author ZhangXingBin
	 * @throws Exception 
	 */
	public TaskEndResult execute(WgTrsTask bean) throws Exception{
		// 获取连接TRS的IP、端口号、用户名以及密码
		String ip = bean.getIp();
		String port = bean.getPort() + "";
		String username = bean.getUsername();
		String password = bean.getPassword();
		String dbName = bean.getDbName();

		// 创建连接TRS数据库对象
		TRSConnection conn = null;

		// 创建TRSResultSet空引用
		TRSResultSet rs = null;

		// 创建TRSDataBase空引用
		TRSDataBase trsDataBase = null;
		
		// 创建MC对象
		MemcachedJava memcachedJava = new MemcachedJava();
		
		try {
			// 建立连接
			conn = new TRSConnection();
			conn.connect(ip, port, username, password, "T10");
			logger.info("数据库连接成功！");

			// 获得全库信息视图对象
			TRSView[] trsView = conn.getViews(dbName);
			// 获取数据库名称
			String databases = trsView[0].getDatabases();
			// 将数据库名称转换为数组
			String[] stringArr = databases.split(";");
			// 将所有的数据库都打印出来
			logger.info(Arrays.toString(stringArr));
			
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			logger.info("开始时间：" + df.format(new Date()));
			
			// 连接MC客户端
			memcachedJava.connection();
			
			// 遍历全库信息视图下的全部数据库
			for (int i = 0; i < stringArr.length; i++) {
				if(i == 2){
					logger.info("去除其ID为2数据库");
					continue;
				}else if(i == 4){
					logger.info("去除其ID为4数据库");
					continue;
				}

				// 去查询指定数据库
				rs = conn.executeSelect(stringArr[i], "", false);
				// 移动到检索结果记录集的第一条记录
				rs.moveFirst();
				TRSRecord record = rs.getRecord();
				// 打印当前数据库ID以及当前数据库名称
				// System.out.println(record.lBaseID + ":" + record.strBaseName);
				
				// 连接当前数据库
				trsDataBase = new TRSDataBase(conn, record.strBaseName);
				
				// 获取当前数据库中的所有字段
				TRSDataBaseColumn[] columns = trsDataBase.getColumns();
				
				// 打印数据库字段
				//logger.info(Arrays.toString(columns));
				
				// 当前数据库总记录数
				long trsCount = rs.getRecordCount();
				// 当前数据库的ID
				String baseId = String.valueOf(record.lBaseID);
				//String baseId = "30";
				// 创建Redis连接实例
				RedisUtil redisUtil = new RedisUtil();
				Jedis jedis = redisUtil.getJedis();
				
				// 判断是否需要增量更新数据
				/**思路：将每个数据库的数据库ID作为key，将该数据库里边的最大记录ID作为value，存入Redis，每次导数据时，将该值个跟trsCount作比较，如果trsCount>value，就进行下述操作，否则不进行*/
				
				// 获取当前数据库以爬取数据最大记录数
				String strBaseId = jedis.get(baseId);
				long currentMaxCount = Long.parseLong(strBaseId);
				
				// 拼装为Memcach需要的数据格式
				/**由于19种数据库，变结构不一样，所以需要逐个进行Memcache数据格式的封装*/
				// 第一种数据库：数据库ID：19
				if(baseId.equals("19")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
							 *  @
								@ACTION:A
								@file_type:0
								@FROM:1
								@MAINCONT:出版时间=>简介=>简介_html=>
								@TITLE:报告名称_栏目
								@BASEID:baseid
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:出版单位
								@DATE:录入时间
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("录入时间")){
									// 获取出版时间
									String data = rs.getString(strColName).toString();
									String values = TrsUtil.date2Long(data);
									if(data == null){
										strbuf.append("@DATE:null\n");
									}else{
										strbuf.append("@DATE:" + values + "\n");
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("简介")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("出版单位")){
									String values = rs.getString(strColName);
									strbuf.append("@AUTHOR:" + values + "\n");
								}else if(strColName.equals("报告名称")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("简介_html")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("出版时间")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("栏目")){
									map.put(strColName, rs.getString(strColName));
								}
								//System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							// 拼装maincont
							String jianJie = map.get("简介").trim();
							String jianJie_html = map.get("简介_html").trim();
							String publicTime = map.get("出版时间");
							strbuf.append("@MAINCONT:出版时间=>" + publicTime + "\n简介=>" + jianJie + "\n简介_html=>" + jianJie_html +"\n");
							// 拼装title
							String titles = map.get("报告名称");
							String channel = map.get("栏目");
							String title = titles+ "_" + channel;
							strbuf.append("@TITLE:" + title + "\n");
							
							// 拼装URL
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("20")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
							 	@
								@ACION:A
								@file_type:0
								@FROM:1
								@AUTHOR:null
								@ABSTRACT:会议名称
								@DATE:录入时间
								@BASEID:baseid 
								@MAINCONT:会议名称 索取号 会议开始时间 会议结束时间
								@TITLE:会议名称_栏目
								@URL:database://数据库名/主键ID
								@DOCID:URLmd5
								@SOURCESTR:数据库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("录入时间")){
									// 获取出版时间
									String data = rs.getString(strColName).toString();
									String values = TrsUtil.date2Long(data);
									if(data == null){
										strbuf.append("@DATE:null\n");
									}else{
										strbuf.append("@DATE:" + values + "\n");
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("索取号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("会议名称")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("栏目")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("会议开始时间")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("会议结束时间")){
									map.put(strColName, rs.getString(strColName));
								}
								//System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							// 拼装MAINCONT
							String meetingNmae = map.get("会议名称");
							String meetingStart = map.get("会议开始时间");
							String meetingEnd = map.get("会议结束时间");
							String getNum = map.get("索取号");
							String maincont = "会议名称=>" + meetingNmae + "\n索取号=>" + getNum + "\n会议开始时间=>" + meetingStart + "\n会议结束时间=>" + meetingEnd;
							strbuf.append("@MAINCONT:" + maincont + "\n");
							strbuf.append("@AUTHOR:null\n");
							
							// 拼装title
							String channel = map.get("栏目");
							String title = meetingNmae+ "_" + channel;
							strbuf.append("@TITLE:" + title + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("17")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
							 	@
								@ACION:A
								@FILE_TYPE:0
								@MAINCONT:网站简介=>网站地址=>开始时间=>到期时间=>网站简介_html=>
								@FROM:1
								@URL:database://数据库名/主键ID
								@URL:URLmd5
								@ABSTRACT:网站简介
								@BASEID:baseid
								@DATE:null
								@TITLE:网站名称_栏目
								@SOURCESTR:数据库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("网站简介")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("网站地址")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("开始时间")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("到期时间")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("网站名称")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("栏目")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("网站简介_html")){
									map.put(strColName, rs.getString(strColName));
								}
								//System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							// MAINCONT拼装
							String jianJie = map.get("网站简介").trim();
							String jianJie_html = map.get("网站简介_html").trim();
							String webUrl = map.get("网站地址");
							String beginTime = map.get("开始时间");
							String endTime = map.get("到期时间");
							String titles = map.get("网站名称");
							String maincont = "网站地址=>" + webUrl + "\n网站名称=>" + titles + "\n网站简介=>" + jianJie + "\n网站简介_html=>" + jianJie_html + "\n开始时间=>" + beginTime + "\n到期时间=>" + endTime; 
							strbuf.append("@MAINCONT:" + maincont + "\n");
							strbuf.append("@DATE:null\n");
							// 拼装title
							String channel = map.get("栏目");
							String title = titles+ "_" + channel;
							strbuf.append("@TITLE:" + title + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}

						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("15")){
					// 馆藏目录库_期刊_中文
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
							 	@
								@ACTION:A
								@file_type:0
								@FROM:1
								@MAINCONT:刊名=> 架位号=> 年=> 卷=> 期=>
								@DATE:录入时间
								@BASEID:baseid
								@TITLE:刊名_栏目
								@ABSTRACT:刊名
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:null
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("录入时间")){
									// 获取出版时间
									String data = rs.getString(strColName).toString();
									String values = TrsUtil.date2Long(data);
									if(data == null){
										strbuf.append("@DATE:null\n");
									}else{
										strbuf.append("@DATE:" + values + "\n");
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("刊名")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("年")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("期")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("卷")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("架位号")){
									map.put(strColName, rs.getString(strColName));
									//strbuf.append("@AUTHOR:" + values + "\n");
								}else if(strColName.equals("栏目")){
									map.put(strColName, rs.getString(strColName));
								}
								//System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							// 拼接@MAINCONT
							String year = map.get("年");
							String titles = map.get("刊名");
							String press = map.get("期");
							String paper = map.get("卷");
							String local = map.get("架位号");
							strbuf.append("@MAINCONT:" + "刊名=>" + titles + "\n年=>" + year+ "\n期=>" + press + "\n卷=>" + paper + "\n架位号=>" + local + "\n");
							strbuf.append("@AUTHOR:null\n");
							// 拼装title
							String channel = map.get("栏目");
							String title = titles+ "_" + channel;
							strbuf.append("@TITLE:" + title + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("21")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
							 	@
								@ACTION:A
								@file_type:0
								@FROM:1
								@MAINCONT:数据库简介_html=> 开始时间=> 到期时间=>
								@DATE:null
								@BASEID:baseid
								@TITLE:数据库名称_栏目
								@ABSTRACT:数据库简介
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:来源
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							strbuf.append("@DATE:null\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("数据库简介")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("来源")){
									String values = rs.getString(strColName);
									strbuf.append("@AUTHOR:" + values + "\n");
								}else if(strColName.equals("数据库名称")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("数据库简介_html")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("开始时间")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("到期时间")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("栏目")){
									map.put(strColName, rs.getString(strColName));
								}
								// System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							// 拼装@MAINCONT
							String jianJie_html = map.get("数据库简介_html");
							String jianJie = map.get("数据库简介");
							String beginTime = map.get("开始时间");
							String endTime = map.get("到期时间");
							strbuf.append("@MAINCONT:数据库简介=>" + jianJie + "/n数据库简介_html=>" + jianJie_html + "\n开始时间=>" + beginTime + "\n到期时间=>" + endTime + "\n");
							// 拼装title
							String titles = map.get("数据库名称");
							String channel = map.get("栏目");
							String title = titles+ "_" + channel;
							strbuf.append("@TITLE:" + title + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("22")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
								@
								@ACTION:A
								@file_type:0
								@FROM:1
								@MAINCONT:图书名称=> 作者=> 出版单位=> 出版时间=> 索取号=>
								@DATE:录入时间
								@BASEID:baseid
								@TITLE:图书名称_栏目
								@ABSTRACT:图书名称
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:作者
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@MAINCONT:\n");
							strbuf.append("@FROM:1\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("录入时间")){
									// 获取出版时间
									String data = rs.getString(strColName).toString();
									String values = TrsUtil.date2Long(data);
									if(data == null){
										strbuf.append("@DATE:null\n");
									}else{
										strbuf.append("@DATE:" + values + "\n");
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("作者")){
									String values = rs.getString(strColName);
									map.put(strColName, rs.getString(strColName));
									strbuf.append("@AUTHOR:" + values + "\n");
								}else if(strColName.equals("出版单位")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("出版时间")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("索取号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("图书名称")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("栏目")){
									map.put(strColName, rs.getString(strColName));
								}
								// System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							// 拼装@ABSTRACT
							String bookName = map.get("图书名称");
							String auth = map.get("作者");
							String publish = map.get("出版单位");
							String time = map.get("出版时间");
							String titles = map.get("索取号");
							String maincont = "图书名称=>" + bookName + "\n作者=>" + auth + "\n出版单位=>" + publish + "\n出版时间=>" + time + "\n索取号=>" + titles;
							strbuf.append("@MAINCONT:" + maincont + "\n");
							// 拼装title
							String channel = map.get("栏目");
							String title = bookName+ "_" + channel;
							strbuf.append("@TITLE:" + title + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("14")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
								@
								@ACTION:A
								@file_type:0
								@FROM:1
								@MAINCONT:英文标题=> 全文=> 附件=> 全文_html=>
								@DATE:发布时间
								@BASEID:baseid
								@TITLE:标题_栏目
								@ABSTRACT:标题
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:null
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							strbuf.append("@AUTHOR:null\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("发布时间")){
									// 获取出版时间
									String data = rs.getString(strColName).toString();
									String values = TrsUtil.date2Long(data);
									if(data == null){
										strbuf.append("@DATE:null\n");
									}else{
										strbuf.append("@DATE:" + values + "\n");
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("标题")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("英文标题")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("全文")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("栏目")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("附件")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("全文_html")){
									map.put(strColName, rs.getString(strColName));
								}
								//System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							// 拼接MAINCONT
							String englishTitle = map.get("英文标题");
							String whole = map.get("全文").trim();
							String appendix = map.get("附件");
							String whole_html = map.get("全文_html").trim();
							String maincont = "英文标题=>" + englishTitle + "\n全文=>" + whole + "\n附件=>" + appendix + "\n全文_html=>" + whole_html + "\n";
							strbuf.append("@MAINCONT:" + maincont + "\n");
							// 拼装title
							String titles = map.get("标题");
							String channel = map.get("栏目");
							String title = titles+ "_" + channel;
							strbuf.append("@TITLE:" + title + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("28")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
								@
								@ACTION:A
								@file_type:0
								@FROM:1 
								@MAINCONT:年份=> 查新目的=> 查新点和要求=> 检索词=> 检索结果=> 查新结论=> 查新项目要点_html=> 检索词_html=> 检索结果_html=>
								@DATE:查新完成日期
								@BASEID:baseid
								@TITLE:项目名称
								@ABSTRACT:查新项目要点
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:委托人
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("查新完成日期")){
									// 获取出版时间
									String data = rs.getString(strColName).toString();
									String values = TrsUtil.date2Long(data);
									if(data == null){
										strbuf.append("@DATE:null\n");
									}else{
										strbuf.append("@DATE:" + values + "\n");
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("委托人")){
									String values = rs.getString(strColName);
									strbuf.append("@AUTHOR:" + values + "\n");
								}else if(strColName.equals("查新项目要点")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("项目名称")){
									String values = rs.getString(strColName);
									strbuf.append("@TITLE:" + values.trim() + "\n");
								}else if(strColName.equals("年份")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("查新目的")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("查新点和要求")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("检索词")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("检索结果")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("查新结论")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("查新项目要点_html")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("检索词_html")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("检索结果_html")){
									map.put(strColName, rs.getString(strColName));
								}
								//System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							// 拼装title
							String year = map.get("年份");
							String ideal = map.get("查新目的");
							String demand = map.get("查新点和要求");
							String indexs = map.get("检索词");
							String result = map.get("检索结果");
							String conclusion = map.get("查新结论");
							String chaxin = map.get("查新项目要点");
							String index_html = map.get("查新项目要点_html");
							String words_html = map.get("检索词_html");
							String result_html = map.get("检索结果_html");
							String title = "年份=>" + year + "\n查新目的=>" + ideal + "\n查新点和要求=>" + demand + 
									"\n检索词=>" + indexs + "\n检索结果=>" + result + "\n查新结论=>" + conclusion + 
									"\n查新项目要点_html=>" + index_html + "\n检索词_html=>" + words_html + "\n检索结果_html=>" + result_html + "\n查新项目要点=>" + chaxin + "\n";
							strbuf.append("@MAINCONT:" + title);
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("40")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
								@
								@ACTION:A
								@file_type:0
								@FROM:1
								@MAINCONT:PA号=> 刊名=> 主题词=> 页数=> 文献语种=> 卷=> 期=> 文献类型=> 专业分类=> 全文=> SYS_FLD_CHECK_DATE=> SYS_FLD_SYSID=> SYS_FLD_FILENAME=> 
								@DATE:年
								@BASEID:baseid
								@TITLE:文献题名
								@ABSTRACT:摘要
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:作者
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							strbuf.append("@DATE:null\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("年")){
									String data = null;
									StringBuffer str = null;
									String values = null;
									try{
										// 获取出版时间
										data = rs.getString(strColName).toString();
										values = TrsUtil.date2Long(data);
										if(data == null){
											strbuf.append("@DATE:null\n");
										}else{
											strbuf.append("@DATE:" + values + "\n");
										}
									}catch(ParseException e){
										logger.error("第" + j + "条数据日期格式异常" + e.getMessage());
										// strbuf.append("@DATE:" + data + "\n");
										try{
											str = new StringBuffer();
											String[] split = data.split("-");
											str.append(split[0]);
											str.append(".");
											str.append("00");
											str.append(".");
											str.append("00");
											try{
												values = TrsUtil.date2Long(str.toString());
												logger.info(values);
												strbuf.append("@DATE:" + values + "\n");
											}catch(ParseException ee){
												logger.error("第" + j + "条数据日期格式异常" + ee.getMessage() + "##该异常较少见，不予解决，做忽略处理！！##");
												
											}
										}catch(Exception se){
											logger.error(se.getMessage() + "该时间格式很异常！");
										}
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("文献题名")){
									String values = rs.getString(strColName);
									strbuf.append("@TITLE:" + values + "\n");
								}else if(strColName.equals("摘要")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("作者")){
									String values = rs.getString(strColName);
									strbuf.append("@AUTHOR:" + values + "\n");
								}else if(strColName.equals("PA号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("主题词")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("页数")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("文献语种")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("刊名")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("卷")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("期")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("文献类型")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("专业分类")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("全文")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("SYS_FLD_CHECK_DATE")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("SYS_FLD_SYSID")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("SYS_FLD_FILENAME")){
									map.put(strColName, rs.getString(strColName));
								}
								//System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							// 拼装MAINCONT
							String PA = map.get("PA号");
							String mainWords = map.get("主题词");
							String time = map.get("期");
							String abstracts = map.get("摘要");
							String pages = map.get("页数");
							String language = map.get("文献语种");
							String day = map.get("刊名");
							String where = map.get("卷");
							String paperType = map.get("文献类型");
							String partion = map.get("专业分类");
							String all = map.get("全文");
							String CHECK_DATE = map.get("SYS_FLD_CHECK_DATE");
							String SYSID = map.get("SYS_FLD_SYSID");
							String FILENAME = map.get("SYS_FLD_FILENAME");
							String title = "PA号=>" + PA + "\n主题词=>" + mainWords + "\n摘要=>" + abstracts + "\n页数=>" + pages + "\n文献语种=>" 
									+ language + "\n期=>" + time + "\n刊名=>" + day + "\n卷=>" + where + "\n文献类型=>" + paperType + "\n专业分类=>"
									+ partion + "\n全文=>" + all + "\nSYS_FLD_CHECK_DATE=>" + CHECK_DATE + "\nSYS_FLD_SYSID=>" + SYSID + "\nSYS_FLD_FILENAME=>" + FILENAME;
							strbuf.append("@MAINCONT:" + title + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}

						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("39")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
								@
								@ACTION:A
								@file_type:0
								@FROM:1
								@MAINCONT:PA号=> 报告号=> 主题词=> 页数=> 文献语种=> 会议日期=> 会议地点=> 文献类型=> 专业分类=> 全文=> SYS_FLD_CHECK_DATE=> SYS_FLD_SYSID=> SYS_FLD_FILENAME=> 
								@DATE:年
								@BASEID:baseid
								@TITLE:会议名称
								@ABSTRACT:摘要
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:作者
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							strbuf.append("@DATE:null\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								String date = null;
								
								if(strColName.equals("会议日期")){
									String values = null;
									try{
										// 获取出版时间
										date = rs.getString(strColName).toString();
										values = TrsUtil.date2Long(date);
										if(date == null){
											strbuf.append("@DATE:null\n");
										}else{
											strbuf.append("@DATE:" + values + "\n");
										}
									}catch(ParseException e){
										logger.error("第" + j + "条数据日期格式异常" + e.getMessage());
										// strbuf.append("@DATE:" + data + "\n");
										StringBuffer str = null;
										try{
											str = new StringBuffer();
											String[] split = date.split("/");
											str.append(split[2]);
											str.append(".");
											str.append(split[0]);
											str.append(".");
											str.append("00");
											try{
												values = TrsUtil.date2Long(str.toString());
												logger.info(values);
												strbuf.append("@DATE:" + values + "\n");
											}catch(ParseException ee){
												logger.error("第" + j + "条数据日期格式异常" + ee.getMessage() + "##该异常较少见，不予解决，做忽略处理！！##");
											}
										}catch(Exception es){
											logger.error("第" + j + "条数据日期格式很异常" + es.getMessage() + date + "##已解决！！！##");
											String strs = date + ".00";
											try{
												values = TrsUtil.date2Long(strs.toString());
												logger.info(values);
												strbuf.append("@DATE:" + values + "\n");
											}catch(ParseException ee){
												logger.error("第" + j + "条数据日期格式异常" + ee.getMessage() + "##该异常较少见，不予解决，做忽略处理！！##");
											}
										}
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("摘要")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("作者")){
									String values = rs.getString(strColName);
									strbuf.append("@AUTHOR:" + values + "\n");
								}else if(strColName.equals("PA号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("报告号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("主题词")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("页数")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("文献语种")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("会议日期")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("会议地点")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("文献类型")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("专业分类")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("全文")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("SYS_FLD_CHECK_DATE")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("SYS_FLD_SYSID")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("SYS_FLD_FILENAME")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("会议名称")){
									String values = rs.getString(strColName);
									strbuf.append("@TITLE:" + values + "\n");
								}
								//System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							// 拼装MAINCONT
							String abstracts = map.get("摘要");
							String PA = map.get("PA号");
							String NO = map.get("报告号");
							String mainWords = map.get("主题词");
							String pages = map.get("页数");
							String language = map.get("文献语种");
							String day = map.get("会议日期");
							String where = map.get("会议地点");
							String paperType = map.get("文献类型");
							String partion = map.get("专业分类");
							String all = map.get("全文");
							String CHECK_DATE = map.get("SYS_FLD_CHECK_DATE");
							String SYSID = map.get("SYS_FLD_SYSID");
							String FILENAME = map.get("SYS_FLD_FILENAME");
							String title = "PA号=>" + PA + "\n报告号=>" + NO + "\n摘要=>" + abstracts + "\n主题词=>" + mainWords + "\n页数=>" + pages + "\n文献语种=>" 
									+ language + "\n会议日期=>" + day + "\n会议地点=>" + where + "\n文献类型=>" + paperType + "\n专业分类=>"
									+ partion + "\n全文=>" + all + "\nSYS_FLD_CHECK_DATE=>" + CHECK_DATE + "\nSYS_FLD_SYSID=>" + SYSID + "\nSYS_FLD_FILENAME=>" + FILENAME;
							strbuf.append("@MAINCONT:" + title + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}

						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("38")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
								@
								@ACTION:A
								@file_type:0
								@FROM:1
								@MAINCONT:副题名=> 文摘号=> 文献类型=> 分类号=> 期刊名=> 年卷期=> 页码=> 
										     作者单位=> 团体作者=> 译者=> 校者=> 会议名称=> 会议时间=> 会议地址=> 
										     主办单位=> 汇编题名=> 申请号=> 专利申请者=> 专利所有者=> 专利公开日=> 
										     专利申请日=> 国际专利号=> 公告号=> 报告号=> 标准号=> 出版者=> 出版地=> 
										     出版日期=> 发布日期=> 实施日期=> 卷书名=> 卷次=> 开本=> 原文出版年=> 
										     译文出处=> 图表参=> 索取号=> 主题词=> 自由词=> 文摘员=> 全文=> 
										  SYS_FLD_CHECK_DATE=> SYS_FLD_SYSID=> SYS_FLD_FILENAME=> 
								@DATE:年
								@BASEID:baseid
								@TITLE:文献题名
								@ABSTRACT:摘要
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:作者
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							strbuf.append("@DATE:null\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("年")){
									try{
										// 获取出版时间
										String data = rs.getString(strColName).toString();
										String values = TrsUtil.date2Long(data);
										if(data == null){
											strbuf.append("@DATE:null\n");
										}else{
											strbuf.append("@DATE:" + values + "\n");
										}
									}catch(ParseException e){
										logger.error("第" + j + "条数据日期格式异常" + e.getMessage());
										// strbuf.append("@DATE:" + data + "\n");
										strbuf.append("@DATE:null\n");
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("摘要")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("文献题名")){
									String values = rs.getString(strColName);
									strbuf.append("@TITLE:" + values + "\n");
								}else if(strColName.equals("作者")){
									String values = rs.getString(strColName);
									strbuf.append("@AUTHOR:" + values + "\n");
								}else if(strColName.equals("副题名")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("文摘号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("文献类型")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("分类号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("期刊名")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("年卷期")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("页码")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("作者单位")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("团体作者")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("译者")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("校者")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("会议名称")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("会议时间")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("会议地址")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("主办单位")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("汇编题名")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("申请号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("专利申请者")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("专利所有者")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("专利公开日")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("专利申请日")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("国际专利号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("公告号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("报告号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("标准号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("出版者")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("出版地")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("出版日期")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("发布日期")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("实施日期")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("卷书名")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("卷次")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("开本")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("原文出版年")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("译文出处")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("图表参")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("索取号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("主题词")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("自由词")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("文摘员")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("全文")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("SYS_FLD_CHECK_DATE")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("SYS_FLD_FILENAME")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("SYS_FLD_SYSID")){
									map.put(strColName, rs.getString(strColName));
								}
								// System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							// 拼装MAINCONT
							String abstracts = map.get("摘要");
							String titles = map.get("副题名");
							String titleNum = map.get("文摘号");
							String type = map.get("文献类型");
							String partNum = map.get("分类号");
							String name = map.get("期刊名");
							String year = map.get("年卷期");
							String apge = map.get("页码");
							String ower = map.get("作者单位");
							String authors = map.get("团体作者");
							String tran = map.get("译者");
							String check = map.get("校者");
							String meetingName = map.get("会议名称");
							String meetingTime = map.get("会议时间");
							String meetingAddress = map.get("会议地址");
							String host = map.get("主办单位");
							String hName = map.get("汇编题名");
							String applyNum = map.get("申请号");
							String applyPeople = map.get("专利申请者");
							String applyOwer = map.get("专利所有者");
							String applyOpenDate = map.get("专利公开日");
							String applyDate = map.get("专利申请日");
							String NationalNum = map.get("国际专利号");
							String openNum = map.get("公告号");
							String orderNum = map.get("报告号");
							String standardNum = map.get("标准号");
							String publish = map.get("出版者");
							String publishAddress = map.get("出版地");
							String publishDate = map.get("出版日期");
							String releaseDate = map.get("发布日期");
							String handleDate = map.get("实施日期");
							String bookName = map.get("卷书名");
							String bookOrder = map.get("卷次");
							String size = map.get("开本");
							String oldPublishYear = map.get("原文出版年");
							String tranWhere = map.get("译文出处");
							String graph = map.get("图表参");
							String getNum = map.get("索取号");
							String titleWords = map.get("主题词");
							String freeWords = map.get("自由词");
							String member = map.get("文摘员");
							String whole = map.get("全文");
							String CHECK_DATE = map.get("SYS_FLD_CHECK_DATE");
							String SYS_FLD_SYSID = map.get("SYS_FLD_SYSID");
							String SYS_FLD_FILENAME = map.get("SYS_FLD_FILENAME");
							String maincont = "副题名=>" + titles + "\n文摘号=>" + titleNum + "\n摘要=>" + abstracts + "\n文献类型=>"+type+"\n分类号=>"+partNum+"\n期刊名=>"+name+"\n年卷期=>"+year+"\n页码=>"+apge+"\n"
									+ "作者单位=>"+ower+"\n团体作者=>"+authors+"\n译者=>"+tran+"\n校者=>"+check+"\n会议名称=>"+meetingName+"\n会议时间=>"+meetingTime+"\n会议地址=>"+meetingAddress+"\n"
									+ "主办单位=>"+host+"\n汇编题名=>"+hName+"\n申请号=>"+applyNum+"\n专利申请者=>"+applyPeople+"\n专利所有者=>"+applyOwer+"\n专利公开日=>"+applyOpenDate+"\n"
									+ "专利申请日=>"+applyDate+"\n国际专利号=>"+NationalNum+"\n公告号=>"+openNum+"\n报告号=>"+orderNum+"\n标准号=>"+standardNum+"\n出版者=>"+publish+"\n出版地=>"+publishAddress+"\n"
									+ "出版日期=>"+publishDate+"\n发布日期=>"+releaseDate+"\n实施日期=>"+handleDate+"\n卷书名=>"+bookName+"\n卷次=>"+bookOrder+"\n开本=>"+size+"\n原文出版年=>"+oldPublishYear+"\n"
									+ "译文出处=>"+tranWhere+"\n图表参=>"+graph+"\n索取号=>"+getNum+"\n主题词=>"+titleWords+"\n自由词=>"+freeWords+"\n文摘员=>"+member+"\n全文=>"+whole+"\n"
									+ "SYS_FLD_CHECK_DATE=>"+CHECK_DATE+"\nSYS_FLD_SYSID=>"+SYS_FLD_SYSID+"\nSYS_FLD_FILENAME=>"+SYS_FLD_FILENAME+"\n";
							strbuf.append("@MAINCONT:" + maincont);
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("37")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
								@
								@ACTION:A
								@file_type:0
								@FROM:1  
								@MAINCONT:标准号或报告号=> 版次=> 出版日期=> 类型=> 语种=> 专业分类=> 页码=> 全文=> 年=> SYS_FLD_CHECK_DATE=> SYS_FLD_FILENAME=>
								@DATE:null
								@BASEID:baseid
								@TITLE:文献题名
								@ABSTRACT:文献题名
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:null
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							strbuf.append("@DATE:null\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("出版时间")){
									try{
										// 获取出版时间
										String data = rs.getString(strColName).toString();
										String values = TrsUtil.date2Long(data);
										if(data == null){
											strbuf.append("@DATE:null\n");
										}else{
											strbuf.append("@DATE:" + values + "\n");
										}
									}catch(ParseException e){
										logger.error("第" + j + "条数据日期格式异常" + e.getMessage());
										// strbuf.append("@DATE:" + data + "\n");
										strbuf.append("@DATE:null\n");
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("文献题名")){
									String values = rs.getString(strColName);
									strbuf.append("@TITLE:" + values + "\n");
								}else if(strColName.equals("标准号或报告号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("版次")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("出版日期")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("类型")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("语种")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("专业分类")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("页码")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("全文")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("年")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("SYS_FLD_CHECK_DATE")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("SYS_FLD_FILENAME")){
									map.put(strColName, rs.getString(strColName));
								}
								//System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							// 拼装title
							String NoNo = map.get("标准号或报告号");
							String realseNo = map.get("版次");
							String publishDate = map.get("出版日期");
							String type = map.get("类型");
							String language = map.get("语种");
							String majorPartion = map.get("专业分类");
							String page = map.get("页码");
							String context = map.get("全文");
							String year = map.get("年");
							String CHECK_DATE = map.get("SYS_FLD_CHECK_DATE");
							String FILENAME = map.get("SYS_FLD_FILENAME");
							String title = "标准号或报告号=>" + NoNo + "\n版次=>" + realseNo + "\n出版日期=>" + publishDate + 
									"\n类型=>" + type + "\n语种=>" + language + "\n专业分类=>" + majorPartion + "\n页码=>" + page + 
									"\n全文=>" + context + "\n年=>" + year + "\nSYS_FLD_CHECK_DATE=>" + CHECK_DATE + "\nSYS_FLD_FILENAME=>" +FILENAME;
							strbuf.append("@MAINCONT:" + title + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("46")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
								@
								@ACTION:A
								@file_type:0
								@FROM:1 
								@MAINCONT:id=> 部门=> RECURL=>
								@DATE:创建时间
								@BASEID:baseid
								@TITLE:标题_栏目
								@ABSTRACT:标题
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:sys_cruser
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("创建时间")){
									// 获取出版时间
									String data = rs.getString(strColName).toString();
									String values = TrsUtil.date2Long(data);
									if(data == null){
										strbuf.append("@DATE:null\n");
									}else{
										strbuf.append("@DATE:" + values + "\n");
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("标题")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("sys_cruser")){
									String values = rs.getString(strColName);
									strbuf.append("@AUTHOR:" + values + "\n");
								}else if(strColName.equals("id")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("部门")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("RECURL")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("栏目")){
									map.put(strColName, rs.getString(strColName));
								}
								//System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							String id = map.get("id");
							String department = map.get("部门");
							String RECURL = map.get("RECURL");
							String maincont = "id=>" + id + "\n部门=>" + department + "\nRECURL=>" + RECURL;
							strbuf.append("@MAINCONT:" + maincont + "\n");
							// 拼装title
							String titles = map.get("标题");
							String channel = map.get("栏目");
							String title = titles+ "_" + channel;
							strbuf.append("@TITLE:" + title + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("31")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
								@
								@ACTION:A
								@file_type:0
								@FROM:1
								@MAINCONT:授予号=> 终止日=> 法律状态=> 法律状态变更=> 专业领域=> 代码=> 申请人地址=> 
										     发明人=> 分案原申请=> 分类号1=> 分类号2=> 公开号=> 公开日=> 国际公布=> 
										     国际申请=> 进入国家日=> 申请号=> 申请人=> 申请日=> 优先权=> 代理机构=> 
										     文摘=> 文摘_html=> sys_cruser=> sys_crtime=>
								@DATE:授予日
								@BASEID:baseid
								@TITLE:标题
								@ABSTRACT:标题
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:代理人
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("授予日")){
									String data = null;
									try{
										// 获取出版时间
										data = rs.getString(strColName).toString();
										String values = TrsUtil.date2Long(data);
										if(data == null){
											strbuf.append("@DATE:null\n");
										}else{
											strbuf.append("@DATE:" + values + "\n");
										}
									}catch(ParseException e){
										logger.error("第" + j + "条数据日期格式异常" + e.getMessage());
										// strbuf.append("@DATE:" + data + "\n");
										try{
											String values = TrsUtil.date2Longs(data);
											strbuf.append("@DATE:" + values + "\n");
										}catch(ParseException ee){
											logger.error("第" + j + "条数据日期格式异常" + e.getMessage());
										}
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("代理人")){
									String values = rs.getString(strColName);
									strbuf.append("@AUTHOR:" + values + "\n");
								}else if(strColName.equals("标题")){
									String values = rs.getString(strColName);
									strbuf.append("@TITLE:" + values + "\n");
								}else if(strColName.equals("授予号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("终止日")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("法律状态")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("法律状态变更")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("专业领域")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("代码")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("申请人地址")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("发明人")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("分案原申请")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("分类号1")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("分类号2")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("公开号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("公开日")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("国际公布")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("国际申请")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("进入国家日")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("申请号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("申请人")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("申请日")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("优先权")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("代理机构")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("文摘")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("文摘_html")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("sys_cruser")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("sys_crtime")){
									map.put(strColName, rs.getString(strColName));
								}
								//System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							// 拼装title
							String giveNum = map.get("授予号");
							String endDate = map.get("终止日");
							String lawStatus = map.get("法律状态");
							String lawChangeStatus = map.get("法律状态变更");
							String professionalField = map.get("专业领域");
							String code = map.get("代码");
							String applyAddress = map.get("申请人地址");
							String adviser = map.get("发明人");
							String oldApply = map.get("分案原申请");
							String partNum1 = map.get("分类号1");
							String partNum2 = map.get("分类号2");
							String openNum = map.get("公开号");
							String openDate = map.get("公开日");
							String nationalOpen = map.get("国际公布");
							String nationalApply = map.get("国际申请");
							String inDate = map.get("进入国家日");
							String applyNum = map.get("申请号");
							String applyPerson = map.get("申请人");
							String applyDate = map.get("申请日");
							String advancePower = map.get("优先权");
							String proxy = map.get("代理机构");
							String digest = map.get("文摘");
							String digest_html = map.get("文摘_html");
							String sys_cruser = map.get("sys_cruser");
							String sys_crtime = map.get("sys_crtime");
							String maincont = "授予号=>" + giveNum + "\n终止日=>" + endDate + "\n法律状态=>" + lawStatus + "\n法律状态变更=>" + lawChangeStatus + "\n专业领域=>" + professionalField + "\n代码=>" + code + 
									"\n申请人地址=>" + applyAddress + "\n发明人=>" + adviser + "\n分案原申请=>" + oldApply + "\n分类号1=>" + partNum1 + "\n分类号2=>" + partNum2 + "\n公开号=>" + openNum + "\n"
									+ "公开日=>" + openDate + "\n国际公布=>" + nationalOpen + "\n国际申请=>" + nationalApply + "\n进入国家日=>" + inDate + "\n申请号=>" + applyNum + "\n申请人=>" + applyPerson + "\n"
									+ "申请日=>" + applyDate + "\n优先权=>" + advancePower + "\n代理机构=>" + proxy + "\n文摘=>" + digest + "\n文摘_html=>" + digest_html + "\n"
									+ "sys_cruser=>" + sys_cruser + "\nsys_crtime=>" + sys_crtime;
							strbuf.append("@MAINCONT:" + maincont + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("41")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
								@
								@ACTION:A
								@file_type:0
								@FROM:1
								@MAINCONT:NewsID=> 正文=>
								@DATE:发布时间
								@BASEID:baseid
								@TITLE:标题_栏目
								@ABSTRACT:标题
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:来源
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("发布时间")){
									// 获取出版时间
									String data = rs.getString(strColName).toString();
									String values = TrsUtil.date2Long(data);
									if(data == null){
										strbuf.append("@DATE:null\n");
									}else{
										strbuf.append("@DATE:" + values + "\n");
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("标题")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("来源")){
									String values = rs.getString(strColName);
									strbuf.append("@AUTHOR:" + values + "\n");
								}else if(strColName.equals("NewsID")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("栏目")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("正文")){
									map.put(strColName, rs.getString(strColName));
								}
								//System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							String newId = map.get("NewsID");
							String article = map.get("正文");
							strbuf.append("@MAINCONT:NewsID=>" + newId + "\n正文=>" + article + "\n");
							// 拼装title
							String titles = map.get("标题");
							String channel = map.get("栏目");
							String title = titles+ "_" + channel;
							strbuf.append("@TITLE:" + title + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("27")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					OilImageData image = new OilImageData();
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
								@
								@ACTION:A
								@file_type:0
								@FROM:1 
								@MAINCONT:FJXXID=> 编号=> 上传用户=> 拍摄时间=> 拍摄地点=> 来源=> 版权所有=> 关键字=> RECURL=> IMGURL=>
								@DATE:创建时间
								@BASEID:baseid
								@TITLE:标题_分类
								@ABSTRACT:概要
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:作者
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("创建时间")){
									// 获取出版时间
									String data = rs.getString(strColName).toString();
									String values = TrsUtil.date2Long(data);
									if(data == null){
										strbuf.append("@DATE:null\n");
									}else{
										strbuf.append("@DATE:" + values + "\n");
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("概要")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("作者")){
									String values = rs.getString(strColName);
									strbuf.append("@AUTHOR:" + values + "\n");
								}else if(strColName.equals("分类")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("标题")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("FJXXID")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("编号")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("上传用户")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("拍摄时间")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("拍摄地点")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("版权所有")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("关键字")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("RECURL")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("IMGURL")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("来源")){
									map.put(strColName, rs.getString(strColName));
								}
								//System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							// 拼接maincont
							String abstracts = map.get("概要");
							String FJXXID = map.get("FJXXID");
							String num = map.get("编号");
							String uploader = map.get("上传用户");
							String time = map.get("拍摄时间");
							String address = map.get("拍摄地点");
							String from = map.get("来源");
							String power = map.get("版权所有");
							String digist = map.get("关键字");
							String recurl = map.get("RECURL");
							String imgurl = map.get("IMGURL");
							image.setImageURL(GetImage.imageBytes(imgurl));
							String fromObj = JSON.toJSONString(image);
							String[] split = fromObj.split("\"");
							String maincont = "FJXXID=>" + FJXXID + "\n摘要=>" + abstracts + "\n编号=>" + num + "\n上传用户=>" + 
									uploader + "\n拍摄时间=>" + time + "\n拍摄地点=>" + address + "\n来源=>" + 
									from + "\n版权所有=>" + power + "\n关键字=>" + digist + "\nRECURL=>" + 
									recurl + "\nIMGURL=>" + imgurl + "\n图片二进制=>" +split[3];
							strbuf.append("@MAINCONT:" + maincont + "\n");
							// 拼装title
							String titles = map.get("分类");
							String channel = map.get("标题");
							String title = titles+ "_" + channel;
							strbuf.append("@TITLE:" + title + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("43")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
								@
								@ACTION:A
								@file_type:0
								@FROM:1
								@MAINCONT:标题=> NewsID=> 地区=> 国家=> 事件类型=> 事件描述=>
								@DATE:发生日期
								@BASEID:baseid
								@TITLE:标题
								@ABSTRACT:标题
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:信息来源
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("发生日期")){
									// 获取出版时间
									String data = rs.getString(strColName).toString();
									String values = TrsUtil.date2Long(data);
									if(data == null){
										strbuf.append("@DATE:null\n");
									}else{
										strbuf.append("@DATE:" + values + "\n");
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("标题")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("信息来源")){
									String values = rs.getString(strColName);
									strbuf.append("@AUTHOR:" + values + "\n");
								}else if(strColName.equals("地区")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("国家")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("事件类型")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("事件描述")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("NewsID")){
									map.put(strColName, rs.getString(strColName));
								}
								//System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							// 拼装title
							String titles = map.get("标题");
							String NewsID = map.get("NewsID");
							String description = map.get("事件描述");
							String type = map.get("事件类型");
							String country = map.get("国家");
							String local = map.get("地区");
							String maincont = "标题=>" + titles + "\nNewsID=>" + NewsID + "\n事件描述=>" + description + "\n事件类型=>" + type + "\n国家=>" + country + "\n地区=>" + local;
							strbuf.append("@MAINCONT:" + maincont + "\n");
							strbuf.append("@TITLE:" + titles + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("44")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
								@
								@ACTION:A
								@file_type:0
								@FROM:1
								@MAINCONT:XXID=> 时间=> 备注=> 地区=> 国家=> 专业=> 产品分类=> 用户=> 附件=>
								@DATE:录入时间
								@BASEID:baseid
								@TITLE:标题_栏目
								@ABSTRACT:标题
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:出处
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							
							strbuf.append("@FROM:1\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("录入时间")){
									// 获取出版时间
									String data = rs.getString(strColName).toString();
									String values = TrsUtil.date2Long(data);
									if(data == null){
										strbuf.append("@DATE:null\n");
									}else{
										strbuf.append("@DATE:" + values + "\n");
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("标题")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("出处")){
									String values = rs.getString(strColName);
									strbuf.append("@AUTHOR:" + values + "\n");
								}else if(strColName.equals("XXID")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("栏目")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("时间")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("备注")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("地区")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("国家")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("专业")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("产品分类")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("附件")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("用户")){
									map.put(strColName, rs.getString(strColName));
								}
								// System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							// 拼装title
							String titles = map.get("标题");
							String channel = map.get("栏目");
							String XXID = map.get("XXID");
							String time = map.get("时间");
							String other = map.get("备注");
							String local = map.get("地区");
							String country = map.get("国家");
							String major = map.get("专业");
							String part = map.get("产品分类");
							String ps = map.get("附件");
							String user = map.get("用户");
							String title = titles+ "_" + channel;
							String maincont = "XXID=>" + XXID + "\n时间=>" + time + "\n备注=>" + other + "\n地区=>" + local + "\n国家=>" + country + "\n专业=>" + major + "\n产品分类=>" + part + "\n用户=>" + user + "\n附件=>" + ps;
							strbuf.append("@MAINCONT:" + maincont + "\n");
							strbuf.append("@TITLE:" + title + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}else if(baseId.equals("45")){
					logger.info("数据库ID：" + baseId + "=>" + trsDataBase);
					// 判断当前数据库中的记录是否大于currentMaxCount
					if(currentMaxCount < trsCount){
						// 遍历当前数据库中所有的记录
						for (long j = currentMaxCount; j < trsCount; j++) {
							// 移动到检索结果记录集的指定记录，在执行此方法后，如果确实移动了记录位置，则那些尚未用update()方法提交的修改将被取消。
							rs.moveTo(0, j);
							
							HashMap<String, String> map = new HashMap<String, String>();
							// 创建一个String缓冲对象
							StringBuffer strbuf = new StringBuffer();
							// 拼装Memcach所需格式
							/**
								@
								@ACTION:A
								@file_type:0
								@FROM:1 
								@MAINCONT:信息内容=> 信息内容_html=> 信息来源=>
								@DATE:sys_crtime
								@BASEID:baseid
								@TITLE:信息标题_信息分类
								@ABSTRACT:信息标题
								@URL: database://库名/主键ID
								@DOCID:URL的md5值（用10进值字符串形式）
								@AUTHOR:sys_cruser
								@SOURCESTR:库名
							 */
							strbuf.append("@\n");
							strbuf.append("@ACTION:A\n");
							strbuf.append("@file_type:0\n");
							strbuf.append("@FROM:1\n");
							
							// 读取当前数据库中某条记录的所有字段
							for (int l = 0; l < columns.length; l++) {
								// 将这个格式（"数据库字段:栏目"）的字段数据元素截取为字段
								String fields = columns[l].toString();
								int index = fields.lastIndexOf(":") + 1;
								String strColName = fields.substring(index);
								
								if(strColName.equals("sys_crtime")){
									// 获取出版时间
									String data = rs.getString(strColName).toString();
									String values = TrsUtil.date2Long(data);
									if(data == null){
										strbuf.append("@DATE:null\n");
									}else{
										strbuf.append("@DATE:" + values + "\n");
									}
								}else if(strColName.equals("baseid")){
									String values = rs.getString(strColName);
									strbuf.append("@BASEID:" + values + "\n");
								}else if(strColName.equals("信息标题")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("sys_cruser")){
									String values = rs.getString(strColName);
									strbuf.append("@AUTHOR:" + values + "\n");
								}else if(strColName.equals("信息分类")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("信息内容")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("信息内容_html")){
									map.put(strColName, rs.getString(strColName));
								}else if(strColName.equals("信息来源")){
									map.put(strColName, rs.getString(strColName));
								}
								//System.out.println(strColName + "-->" + rs.getString(strColName).toString());
							}
							
							String info = map.get("信息内容");
							String info_html = map.get("信息内容_html");
							String infoFrom = map.get("信息来源");
							String maincont = "信息内容=>"+ info+"\n信息内容_html=>" + info_html + "\n信息来源=>" +infoFrom;
							strbuf.append("@MAINCONT:" + maincont + "\n");
							// 拼装title
							String titles = map.get("信息标题");
							String channel = map.get("信息分类");
							String title = titles+ "_" + channel;
							strbuf.append("@TITLE:" + title + "\n");
							long index = j + 1;
							String url = "database://" + record.strBaseName + "/" + index;
							strbuf.append("@URL:" + url + "\n");
							String docidDM5 = TrsMD5.MD5Encode(url);
							strbuf.append("@DOCID:" + docidDM5 + "\n");
							strbuf.append("@SOURCESTR:" + record.strBaseName + "\n");
							
							// 最终Memcach数据串
							memcachedJava.Memcached(strbuf.toString());
						}
						
						// Redis要将最大记录值保存至Redis中
						String preMaxCount = String.valueOf(trsCount);
						jedis.set(baseId, preMaxCount);
					}else{
						logger.info(record.strBaseName + "中没有数据增量");
					}
				}
				
				// 回收Redis资源
				redisUtil.returnResource(jedis);
			}

		} catch (TRSException e) {
			logger.error("Trs数据库连接失败---->" + e.getErrorString());
		} finally {
			// 关闭结果集
			if (rs != null) {
				rs.close();
			}
			rs = null;
			
			// 关闭连接
			if (conn != null){
				conn.close();
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				logger.info("结束时间：" + df.format(new Date()));
				logger.info("数据导入完毕");
			}
			conn = null;
			
			// 关闭MC
			if(memcachedJava != null){
				memcachedJava.loseMC();
			}
			memcachedJava = null;
		}
		return null;
	}
}
