package get.entity;

import java.io.Serializable;

/**
 * 图片库
 * 
 * @author Gnahznib
 * 
 */
public class OilImageData implements Serializable {

	private static final long serialVersionUID = 1L;

	// 物理记录号
	private String docId;

	// 附件信息ID
	private String annexInformationID;

	// 分类
	private String classification;

	// 标题
	private String title;

	// 标号
	private String grade;

	// 作者
	private String author;

	// 创建用户
	private String createUser;

	// 拍摄时间
	private String recordingTime;

	// 拍摄地点
	private String filmingLocations;

	// 来源
	private String source;

	// 概要
	private String summary;

	// 版权所有
	private String allRightsReserved;

	// 关键字
	private String categories;

	// 原文链接URL
	private String originalURL;

	// 图片URL
	private byte[] imageURL;

	// baseid
	private String baseId;

	// 创建时间
	private String createTime;

	// 上传时间
	private String uploadTime;

	// 上传用户
	private String uploadUser;

	// 数据源编号(任务ID)
	private String dataSourceCode;

	public String getDataSourceCode() {
		return dataSourceCode;
	}

	public void setDataSourceCode(String dataSourceCode) {
		this.dataSourceCode = dataSourceCode;
	}

	public String getAnnexInformationID() {
		return annexInformationID;
	}

	public void setAnnexInformationID(String annexInformationID) {
		this.annexInformationID = annexInformationID;
	}

	public String getClassification() {
		return classification;
	}

	public void setClassification(String classification) {
		this.classification = classification;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getGrade() {
		return grade;
	}

	public String getDocId() {
		return docId;
	}

	public void setDocId(String docId) {
		this.docId = docId;
	}

	public void setGrade(String grade) {
		this.grade = grade;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public String getCreateUser() {
		return createUser;
	}

	public void setCreateUser(String createUser) {
		this.createUser = createUser;
	}

	public String getFilmingLocations() {
		return filmingLocations;
	}

	public void setFilmingLocations(String filmingLocations) {
		this.filmingLocations = filmingLocations;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}

	public String getAllRightsReserved() {
		return allRightsReserved;
	}

	public void setAllRightsReserved(String allRightsReserved) {
		this.allRightsReserved = allRightsReserved;
	}

	public String getCategories() {
		return categories;
	}

	public void setCategories(String categories) {
		this.categories = categories;
	}

	public String getOriginalURL() {
		return originalURL;
	}

	public void setOriginalURL(String originalURL) {
		this.originalURL = originalURL;
	}

	public byte[] getImageURL() {
		return imageURL;
	}

	public void setImageURL(byte[] imageURL) {
		this.imageURL = imageURL;
	}

	public String getBaseId() {
		return baseId;
	}

	public void setBaseId(String baseId) {
		this.baseId = baseId;
	}

	public String getUploadTime() {
		return uploadTime;
	}

	public void setUploadTime(String uploadTime) {
		this.uploadTime = uploadTime;
	}

	public String getUploadUser() {
		return uploadUser;
	}

	public void setUploadUser(String uploadUser) {
		this.uploadUser = uploadUser;
	}

	public String getRecordingTime() {
		return recordingTime;
	}

	public void setRecordingTime(String recordingTime) {
		this.recordingTime = recordingTime;
	}

	public String getCreateTime() {
		return createTime;
	}

	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}
}