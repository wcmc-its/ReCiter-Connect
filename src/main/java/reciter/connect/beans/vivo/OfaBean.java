package reciter.connect.beans.vivo;

import java.util.ArrayList;

/**
 * @author Sarbajit Dutta (szd2013@med.cornell.edu)
 * <p>This class is a bean class containing both the role and education & training information from faculty. The source for appointmens comes from ED
 * and source for education and training is from OFA database</p>
 */
public class OfaBean {
	

	private String cwid;
	private String primaryAffiliation;
	private ArrayList<RoleBean> roles = new ArrayList<RoleBean>();
	private ArrayList<EducationBean> edu = new ArrayList<EducationBean>();
	
	
	
	
	
	public ArrayList<EducationBean> getEdu() {
		return edu;
	}
	public void setEdu(ArrayList<EducationBean> edu) {
		this.edu = edu;
	}
	public ArrayList<RoleBean> getRoles() {
		return roles;
	}
	public void setRoles(ArrayList<RoleBean> roles) {
		this.roles = roles;
	}
	public String getPrimaryAffiliation() {
		return primaryAffiliation;
	}
	public void setPrimaryAffiliation(String primaryAffiliation) {
		this.primaryAffiliation = primaryAffiliation;
	}
	
	
	public String getCwid() {
		return cwid;
	}
	public void setCwid(String cwid) {
		this.cwid = cwid;
	}
	
		
}
