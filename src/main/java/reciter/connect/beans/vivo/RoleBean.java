package reciter.connect.beans.vivo;


/**
 * @author Sarbajit Dutta (szd2013@med.cornell.edu)
 * <p>This class is a bean class containing all the role related information from ED</p>
 */
public class RoleBean implements Comparable<RoleBean> {
	
	private String sorId;
	private String startDate;
	private String endDate;
	private String titleCode;
	private String department;
	private int deptCode;
	private boolean isPrimaryAppointment;
	private String crudStatus;
	private boolean isInterimAppointment;
	private boolean isActiveAppointment;
	
	
	
	

	public boolean isPrimaryAppointment() {
		return isPrimaryAppointment;
	}
	public void setPrimaryAppointment(boolean isPrimaryAppointment) {
		this.isPrimaryAppointment = isPrimaryAppointment;
	}
	public String getSorId() {
		return sorId;
	}
	public void setSorId(String sorId) {
		this.sorId = sorId;
	}
	public String getStartDate() {
		return startDate;
	}
	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}
	public String getEndDate() {
		return endDate;
	}
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	public String getTitleCode() {
		return titleCode;
	}
	public void setTitleCode(String titleCode) {
		this.titleCode = titleCode;
	}
	public String getDepartment() {
		return department;
	}
	public void setDepartment(String department) {
		this.department = department;
	}
	public int getDeptCode() {
		return deptCode;
	}
	public void setDeptCode(int deptCode) {
		this.deptCode = deptCode;
	}
	public String getCrudStatus() {
		return crudStatus;
	}
	public void setCrudStatus(String crudStatus) {
		this.crudStatus = crudStatus;
	}
	
	public boolean isInterimAppointment() {
		return isInterimAppointment;
	}
	public void setInterimAppointment(boolean isInterimAppointment) {
		this.isInterimAppointment = isInterimAppointment;
	}
	
	public boolean isActiveAppointment() {
		return isActiveAppointment;
	}
	public void setActiveAppointment(boolean isActiveAppointment) {
		this.isActiveAppointment = isActiveAppointment;
	}
	public String toString() {
		return this.sorId + " " + this.titleCode + " " + this.department + " " + this.startDate + " " + this.endDate + " " + this.deptCode + " " + this.isPrimaryAppointment + " " + this.isInterimAppointment;
	}
	
	
	public int compareTo(RoleBean rb) {
		//System.out.println(rb.getDepartment() + " && " + this.department + "::" + rb.getTitleCode() + " && " + this.titleCode);
		return (rb.getDepartment().equals(this.department) && rb.getTitleCode().equals(this.titleCode)?1:0);
	}
	
	
}
