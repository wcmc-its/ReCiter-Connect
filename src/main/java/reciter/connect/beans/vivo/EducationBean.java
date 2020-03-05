package reciter.connect.beans.vivo;

/**
 * @author Sarbajit Dutta (szd2013@med.cornell.edu)
 *	<p>This class contains education & training data from OFA oracle database.</p>
 */
public class EducationBean {
	
	private String degreePk;
	private String degreeName;
	private String instituion;
	private String instituteFk;
	private String builtInDegreePk;
	private String dateTimeInterval;
	private String crudStatus;
	
	public String getDegreePk() {
		return degreePk;
	}
	public void setDegreePk(String degreePk) {
		this.degreePk = degreePk;
	}
	public String getDegreeName() {
		return degreeName;
	}
	public void setDegreeName(String degreeName) {
		this.degreeName = degreeName;
	}
	public String getInstituion() {
		return instituion;
	}
	public void setInstituion(String instituion) {
		this.instituion = instituion;
	}
	public String getInstituteFk() {
		return instituteFk;
	}
	public void setInstituteFk(String instituteFk) {
		this.instituteFk = instituteFk;
	}
	public String getBuiltInDegreePk() {
		return builtInDegreePk;
	}
	public void setBuiltInDegreePk(String builtInDegreePk) {
		this.builtInDegreePk = builtInDegreePk;
	}
	public String getDateTimeInterval() {
		return dateTimeInterval;
	}
	public void setDateTimeInterval(String dateTimeInterval) {
		this.dateTimeInterval = dateTimeInterval;
	}
	public String getCrudStatus() {
		return crudStatus;
	}
	public void setCrudStatus(String crudStatus) {
		this.crudStatus = crudStatus;
	}
	
	public String toString() {
		return this.degreePk + " " + this.builtInDegreePk + " " + this.degreeName + " " + this.instituteFk + " " + this.instituion + " " + this.dateTimeInterval;
	}
	
	
}
