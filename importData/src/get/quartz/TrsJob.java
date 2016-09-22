package get.quartz;

import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import get.entity.WgTrsTask;
import get.link.LinkTRS;
import get.util.PropertyUtils;

/**
 * @Package: readTrs.quartz
 * @Description:quartz框架的Job
 * @Version: V0.1
 * @Author: ZhangXingBin
 * @ChangeHistoryList: Version     Author      		Date                    Description
 *                     V0.1        ZhangXingBin     2016年3月10日 下午4:42:33
 */
public class TrsJob implements Job{
	private static WgTrsTask trsTask = null;
	private static LinkTRS trs = null;	
	private static Logger logger = Logger.getLogger(TrsJob.class);
	// 静态代码块确保Trs数据库连接
	static{
		trsTask = new WgTrsTask();
		trs = new LinkTRS();
		trsTask.setIp(PropertyUtils.getBasicConfig("ip"));
		trsTask.setPassword(PropertyUtils.getBasicConfig("password"));
		trsTask.setPort(Integer.parseInt(PropertyUtils.getBasicConfig("port")));
		trsTask.setUsername(PropertyUtils.getBasicConfig("username"));//system
		trsTask.setDbName(PropertyUtils.getBasicConfig("dbname"));	// 全库信息数据
		//trsTask.setDbName("szxsj");	// 数值型数据
	}
	
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		try {
			// 执行Trs数据库数据查询
			trs.execute(trsTask);
		} catch (Exception e) {
			logger.error("执行Job任务异常---->" + e.getMessage());
			e.printStackTrace();
		}
	}
}
