package reciter.connect.beans.vivo;

import java.util.List;
import java.util.Map;

public class GrantBean {
	
	private String cwid;
	private String awardNumber;
	private String sponsorAwardNumber;
	private String title;
	private String beginDate;
	private String endDate;
	private String department;
	private String departmentName;
	private String sponsorName;
	private String sponsorCode;
	private String primeSponsorName;
	private Map<String, String> contributors;
	private boolean unitCodeMissing;
	
	
	
	public String getDepartmentName() {
		return departmentName;
	}
	public void setDepartmentName(String departmentName) {
		this.departmentName = departmentName;
	}
	public boolean isUnitCodeMissing() {
		return unitCodeMissing;
	}
	public void setUnitCodeMissing(boolean unitCodeMissing) {
		this.unitCodeMissing = unitCodeMissing;
	}
	public String getSponsorCode() {
		return sponsorCode;
	}
	public void setSponsorCode(String sponsorCode) {
		this.sponsorCode = sponsorCode;
	}
	public String getCwid() {
		return cwid;
	}
	public void setCwid(String cwid) {
		this.cwid = cwid;
	}
	public String getAwardNumber() {
		return awardNumber;
	}
	public void setAwardNumber(String awardNumber) {
		this.awardNumber = awardNumber;
	}
	public String getSponsorAwardNumber() {
		return sponsorAwardNumber;
	}
	public void setSponsorAwardNumber(String sponsorAwardNumber) {
		this.sponsorAwardNumber = sponsorAwardNumber;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getBeginDate() {
		return beginDate;
	}
	public void setBeginDate(String beginDate) {
		this.beginDate = beginDate;
	}
	public String getEndDate() {
		return endDate;
	}
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	public String getDepartment() {
		return department;
	}
	public void setDepartment(String department) {
		this.department = department;
	}
	public String getSponsorName() {
		return sponsorName;
	}
	public void setSponsorName(String sponsorName) {
		this.sponsorName = sponsorName;
	}
	public String getPrimeSponsorName() {
		return primeSponsorName;
	}
	public void setPrimeSponsorName(String primeSponsorName) {
		this.primeSponsorName = primeSponsorName;
	}
	public Map<String, String> getContributors() {
		return contributors;
	}
	public void setContributors(Map<String, String> contributors) {
		this.contributors = contributors;
	}
	@Override
	public String toString() {
		return this.awardNumber + " " + this.title + " " + this.department + " " + this.sponsorName + " " + this.primeSponsorName + " " + this.beginDate + " " + this.endDate;
	}
	
	@Override
	public boolean  equals (Object object) {
	boolean result = false;
	if (object == null || object.getClass() != getClass()) {
	    result = false;
	} else {
		GrantBean gb = (GrantBean) object;
	    if (this.awardNumber.equals(gb.getAwardNumber())) {
	        result = true;
	    }
	}
	return result;
	}
	
	
}
