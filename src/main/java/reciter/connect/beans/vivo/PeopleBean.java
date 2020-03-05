package reciter.connect.beans.vivo;

import java.util.ArrayList;
import org.openjena.atlas.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeopleBean {
	
	private String cwid;
	private String personCode;
	private String status;
	private String primaryTitle;
	private String telephoneNumber;
	private String mail;
	private String displayName;
	private String sn;
	private String middleName;
	private String givenName;
	private String popsProfile;
	private ArrayList<String> nsTypes;
	
	private static Logger log = LoggerFactory.getLogger(PeopleBean.class);
	
	public PeopleBean() {
		// TODO Auto-generated constructor stub
		
	
	}
	
	
	public String getCwid() {
		return cwid;
	}
	public void setCwid(String cwid) {
		this.cwid = cwid;
	}
	public String getPersonCode() {
		return personCode;
	}
	public void setPersonCode(String personCode) {
		this.personCode = personCode;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getPrimaryTitle() {
		return primaryTitle;
	}
	public void setPrimaryTitle(String primaryTitle) {
		this.primaryTitle = primaryTitle;
	}
	public String getTelephoneNumber() {
		return telephoneNumber;
	}
	public void setTelephoneNumber(String telephoneNumber) {
		this.telephoneNumber = telephoneNumber;
	}
	public String getMail() {
		return mail;
	}
	public void setMail(String mail) {
		this.mail = mail;
	}
	public String getDisplayName() {
		return displayName;
	}
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}
	public String getSn() {
		return sn;
	}
	public void setSn(String sn) {
		this.sn = sn;
	}
	public String getMiddleName() {
		return middleName;
	}
	public void setMiddleName(String middleName) {
		this.middleName = middleName;
	}
	public String getGivenName() {
		return givenName;
	}
	public void setGivenName(String givenName) {
		this.givenName = givenName;
	}
	public String getPopsProfile() {
		return popsProfile;
	}
	public void setPopsProfile(String popsProfile) {
		this.popsProfile = popsProfile;
	}


	public ArrayList<String> getNsTypes() {
		return nsTypes;
	}


	public void setNsTypes(ArrayList<String> nsTypes) {
		this.nsTypes = nsTypes;
	}
	


	public String toString() {
		return this.cwid + " " + this.displayName + " " + this.mail + " " + this.telephoneNumber + " " + this.primaryTitle + " " + this.personCode + " " + this.sn + " " + this.middleName + " " + this.givenName + "-" + this.popsProfile;
	}
	
}
