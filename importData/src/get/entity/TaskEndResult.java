package get.entity;

import java.util.Date;
/**
 * 任务结束后发送至web端接口的bean对象
 * @author gnahznib
 *
 */
public class TaskEndResult {
	//唯一标示
	public String sid;
	//任务id
	public String dataSourceCode;
	//任务状态
	public String resultCode;
	//任务结果描述
	public String resultMsg;
	//任务结束时间
	public Date endTime;
	//任务开始时间
	public Date startTime;
	
	public int recordID;
	
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
	public String getResultCode() {
		return resultCode;
	}
	public void setResultCode(String resultCode) {
		this.resultCode = resultCode;
	}
	public String getResultMsg() {
		return resultMsg;
	}
	public void setResultMsg(String resultMsg) {
		this.resultMsg = resultMsg;
	}
	public Date getEndTime() {
		return endTime;
	}
	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}
	public Date getStartTime() {
		return startTime;
	}
	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}
	
}
