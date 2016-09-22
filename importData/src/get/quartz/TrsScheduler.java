package get.quartz;

import org.apache.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import get.util.PropertyUtils;

/**
 * @Description 通过该调度器来触发每天的数据导入事件，每天导入时间是凌晨0:00，数据导入方式为增量导入
 * @author ZhangXingBin
 *
 */
public class TrsScheduler {
	private static Logger logger = Logger.getLogger(TrsScheduler.class);
	
	public static void main(String[] args) {

		// 通过schedulerFactory获取一个调度器
		SchedulerFactory schedulerfactory = new StdSchedulerFactory();
		Scheduler scheduler = null;
		try {
			// 通过schedulerFactory获取一个调度器
			scheduler = schedulerfactory.getScheduler();

			// 创建jobDetail实例，绑定Job实现类
			// 指明job的名称，所在组的名称，以及绑定job类
			JobDetail job = JobBuilder.newJob(TrsJob.class)
					.withIdentity("TrsJob", "TrsJobGroup").build();

			// 定义调度触发规则
			// 使用cornTrigger规则 每天10点42分
			Trigger trigger = TriggerBuilder
					.newTrigger()
					.withIdentity("simpleTrigger", "triggerGroup")
					.withSchedule(CronScheduleBuilder.cronSchedule(PropertyUtils.getBasicConfig("runtime")))
					.startNow().build();

			// 把作业和触发器注册到任务调度中
			scheduler.scheduleJob(job, trigger);

			// 启动调度
			scheduler.start();

		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
}