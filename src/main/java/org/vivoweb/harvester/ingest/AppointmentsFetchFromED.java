package org.vivoweb.harvester.ingest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import reciter.connect.beans.vivo.*;
import reciter.connect.database.ldap.LDAPConnectionFactory;
import reciter.connect.database.mssql.MssqlConnectionFactory;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;
import reciter.connect.database.tdb.TDBConnectionFactory;
import reciter.connect.vivo.IngestType;
import reciter.connect.vivo.api.client.VivoClient;

import org.vivoweb.harvester.util.repo.SDBJenaConnect;
import org.vivoweb.harvester.util.repo.TDBJenaConnect;

import lombok.extern.slf4j.Slf4j;

import com.unboundid.ldap.sdk.SearchResultEntry;


/**
 * @author Sarbajit Dutta (szd2013@med.cornell.edu)
 * <p>This class fetches appointments from Enterprise Directory and Education & Training data from OFA and imports or updates in VIVO.
 * Since the people data comes from ED the data that is used by this class is for faculty who are active in ED.
 * Also, the organization structure uses the old org hierarchy scripts used in D2RMAP. For updates the data checks for change in institution for education and if a position 
 * has been updated from current to expired i.e. has an end date and addition of any new education or training. The data is automatically inserted into the inference
 * graph. (for now).
 * </p>
 */
@Slf4j
@Component
public class AppointmentsFetchFromED {
	
	/**
	 * LDAP connection factory for all enterprise directory related connections
	 */
	@Autowired
	private LDAPConnectionFactory lcf;
	
	private Connection con = null;
	
	/**
	 * Jena connection factory object for all the apache jena sdb related connections
	 */
	@Autowired
	private JenaConnectionFactory jcf;

	@Autowired
	private TDBConnectionFactory tcf;

	@Autowired
	private VivoClient vivoClient;

	private String ingestType = System.getenv("INGEST_TYPE");

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	/**
	 * The default namespace for VIVO
	 */
	private String vivoNamespace = TDBConnectionFactory.nameSpace;

	private String currYear = new SimpleDateFormat("yyyy").format(new Date());

	
	/**
	 * Mssql connection factory object for all the mysql related connections
	 */
	@Autowired
	private MssqlConnectionFactory mcf; 

	public Callable<String> getCallable(List<String> people, Connection asmsCon) {
		this.con = asmsCon;
        return new Callable<String>() {
            public String call() throws Exception {
                return execute(people);
            }
        };
    }
		
		/**
		 * This is the main execution method of the class
		 * @throws IOException when connecting to ED
		 */
	public String execute(List<String> people) throws IOException {
		StopWatch stopWatch = new StopWatch("Appointments fetch performance");
		stopWatch.start("Appointments updates");
		int insertCount = 0;
		int updateCount = 0;
		List<OfaBean> ofaData = new ArrayList<>();
		//Initialize connection pool and fill it with connection
		Iterator<String> it = people.iterator();
		while(it.hasNext()) {
			String cwid = it.next();
			OfaBean ob = getRolesFromED(cwid); //Get all the appointments in ED
			ArrayList<EducationBean> edu = getEducationAndTraining(ob.getCwid()); //Get all the education and training data from OFA
			ob.setEdu(edu);
			ofaData.add(ob);
		}
		
		Iterator<OfaBean>  it1 = ofaData.iterator();
		while(it1.hasNext()) {
			OfaBean ob1 = it1.next();
			log.info("#########################################################");
			if(!checkOfaDataInVivo(ob1)) {
				log.info("Person: "+ob1.getCwid() + " does not has appointments in VIVO");
				insertOfaDataInVivo(ob1);
				
				insertCount = 	insertCount + 1;
			}
			else {
				log.info("Checking for any updates for "+ob1.getCwid());
				if(ingestType.equals(IngestType.VIVO_API.toString())) {
					updateCount = checkForUpdatesUsingTDB(ob1, ob1.getCwid());
				} else {
					updateCount = checkForUpdates(ob1, ob1.getCwid());
				}
				
			}
			log.info("#########################################################");
		}

		
		log.info("New appointments fetched for " + insertCount + " people");
		log.info("Appointments updated for " + updateCount + " people");
		log.info("Appointments fetch completed successfully...");
		stopWatch.stop();
		log.info("Appointment fetch Time taken: " + stopWatch.getTotalTimeSeconds() + "s");
		return "Appointment fetch completed successfully for cwids: " + people.toString();
	}
		
		/**
		 * This function gets all the appointment information from your LDAP based system data
		 * @param cwid This is the institution wide unique identifier for a person
		 * @return The bean which holds appointment and education data
		 */
		private OfaBean getRolesFromED(String cwid) {
			Date now = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			String strDate = sdf.format(now);
			OfaBean ob = new OfaBean();
			ArrayList<RoleBean> roles = new ArrayList<RoleBean>();
			
			ob.setCwid(cwid);
			log.info("Getting list of appointments for cwid " + cwid);
			log.info("Getting primary Affiliation for cwid " + cwid);	
			String filter = "(&(ou=faculty)(objectClass=weillCornellEduSORRecord)(weillCornellEduCWID=" + cwid + "))";
				
				
				
			List<SearchResultEntry> results = this.lcf.searchWithBaseDN(filter,"ou=faculty,ou=sors,dc=weill,dc=cornell,dc=edu");
				
				String primaryAffiliation = null;
				String primaryPosition = null;
				
				/* if (results != null) {*/
				if (results != null && !results.isEmpty()) {
					for (SearchResultEntry entry : results) {
						if(entry.getAttributeValue("weillCornellEduPrimaryOrganization") != null) {
							
							if(entry.getAttributeValue("weillCornellEduPrimaryOrganization") != null)
								primaryAffiliation = entry.getAttributeValue("weillCornellEduPrimaryOrganization");
							
							if(entry.getAttributeValue("weillCornellEduPrimaryTitle") != null)
								primaryPosition = entry.getAttributeValue("weillCornellEduPrimaryTitle");
							
							log.info("Primary Position: " + primaryPosition);
								
						}
					}
					log.info("Number of results found: " + results.size());
				}
				else {
					log.info("No results found");
				}
			
			//Takes care of Douglas J. Ballon appointments			
			filter = "(&(ou=faculty)(objectClass=weillCornellEduSORRoleRecord)(weillCornellEduCWID=" + cwid + ")(!(weillCornellEduTitleCode=academic-prestart))(weillCornellEduEndDate>=19991231050000Z)(!(|(weillCornellEduSORID=10085791)(weillCornellEduSORID=2318)(weillCornellEduSORID=10002523)(weillCornellEduSORID=10075683)(weillCornellEduSORID=10016608)(weillCornellEduSORID=3001124))))";

			
			results = this.lcf.searchWithBaseDN(filter,"ou=faculty,ou=sors,dc=weill,dc=cornell,dc=edu");
			
			if (results != null && !results.isEmpty()) {
				for (SearchResultEntry entry : results) {
					if(entry.getAttributeValue("weillCornellEduCWID") != null) {
						Date endDate = null;
						Date currDate = null;
						RoleBean rb = new RoleBean();
						rb.setSorId(entry.getAttributeValue("weillCornellEduSORID"));
						rb.setDepartment(entry.getAttributeValue("weillCornellEduDepartment"));
						rb.setTitleCode(entry.getAttributeValue("title"));
						rb.setStartDate(entry.getAttributeValue("weillCornellEduStartDate").substring(0, 4));
						String ldapEndDate = entry.getAttributeValue("weillCornellEduEndDate").substring(0, 4) + "-" + entry.getAttributeValue("weillCornellEduEndDate").substring(4, 6) + "-" + entry.getAttributeValue("weillCornellEduEndDate").substring(6, 8);
						try {
							log.info("Ldap end date: " + ldapEndDate);
							if(ldapEndDate.matches("^\\d{4}-\\d{2}-\\d{2}$") && ldapEndDate.length() > 2)
								endDate = sdf.parse(ldapEndDate);
							
							log.info("current date: " + strDate);
							if(strDate.matches("^\\d{4}-\\d{2}-\\d{2}$") && strDate.length() > 2)
								currDate = sdf.parse(strDate);
						} catch(ParseException e) {
							log.error("ParseException", e);
						}
						
						if(rb.getTitleCode().contains("(Interim)")) {
							rb.setInterimAppointment(true);
						}
						
						if(entry.getAttributeValue("weillCornellEduStatus").equalsIgnoreCase("faculty:active") || entry.getAttributeValue("weillCornellEduStatus").equalsIgnoreCase("academic:active")) {
							rb.setActiveAppointment(true);
						}
						
						if(endDate != null && endDate.compareTo(currDate) >= 0)
							rb.setEndDate("CURRENT");//This means the appointment is current and does not have an end date
						else
							rb.setEndDate(entry.getAttributeValue("weillCornellEduEndDate").substring(0, 4));
						
						
						if(entry.getAttributeValue("weillCornellEduDepartment") != null) {
							rb.setDeptCode(getDepartmentCode(entry.getAttributeValue("weillCornellEduDepartment")));
						}
						//Determining Primary Position
						if(primaryPosition == null) {
							if(entry.getAttributeValue("weillCornellEduPrimaryEntry").trim().equals("TRUE"))
								rb.setPrimaryAppointment(true);
							else 
								rb.setPrimaryAppointment(false);
						}
						else {
							if(entry.getAttributeValue("title").trim().equals(primaryPosition.trim())) {
								rb.setPrimaryAppointment(true);
							}
							else 
								rb.setPrimaryAppointment(false);
						}
						
						roles.add(rb); 
					}
				}
			}
			else {
				log.info("No results found");
			}
			
			ArrayList<RoleBean> modifiedRoles = checkForTenureTrack(roles);
			
			
			
			
			log.info("Getting verbose equivalent for " + primaryAffiliation);
			/*filter = "(o=" + primaryAffiliation + ")";
			String basedn = "ou=organizations,ou=groups,dc=weill,dc=cornell,dc=edu";
			results = this.lcf.searchWithBaseDN(filter,basedn);
			
			if (results != null) {
				
		           
	            for (SearchResultEntry entry : results) {
	            	if(entry.getAttributeValue("cn") != null) {
	                  
	                  primaryAffiliation = entry.getAttributeValue("cn");
	                     
	            	}
	            }
	        }
	        else {
	            log.info("No results found");
	        }*/
			if(primaryAffiliation != null && primaryAffiliation.length() > 0) {
				switch (primaryAffiliation) {
					case "MSKCC":
						primaryAffiliation = "Memorial Sloan Kettering Cancer Center";
						break;
					case "WCMC":
						primaryAffiliation = "Weill Cornell Medical College";
						break;
					case "CUCPS":
						primaryAffiliation = "Columbia University College of Physicians and Surgeons";
						break;
					case "NYP":
						primaryAffiliation = "New York-Presbyterian Hospital";
						break;
					case "WCMC-Q":
						primaryAffiliation = "Weill Cornell Medical College in Qatar";
						break;
					case "NYMH":
						primaryAffiliation = "New York Methodist Hospital";
						break;
					case "HSS":
						primaryAffiliation = "Hospital for Special Surgery";
						break;
					case "NYPQ":
						primaryAffiliation = "New York Presbyterian - Queens";
						break;
					case "RI":
						primaryAffiliation = "Rogosin Institute";
						break;
					case "SIDRA":
						primaryAffiliation = "SIDRA Medical and Research Center";
						break;
					case "HMC":
						primaryAffiliation = "Hamad Medical Corporation";
						break;
					case "WMBMRI":
						primaryAffiliation = "Winifred Masterson Burke Medical Research Institute";
						break;
					case "HMH":
						primaryAffiliation = "Houston Methodist Hospital";
						break;
					case "RU":
						primaryAffiliation = "Rockefeller University";
						break;
					case "LMMHC":
						primaryAffiliation = "Lincoln Medical and Mental Health Center";
						break;
					case "Cornell":
						primaryAffiliation = "Cornell University";
						break;
					case "AspH":
						primaryAffiliation = "Aspetar Hospital";
						break;
					case "CMCIthaca":
						primaryAffiliation = "Cayuga Medical Center of Ithaca";
						break;
					case "PHCC":
						primaryAffiliation = "Primary Health Care Corporation (Qatar)";
						break;
					case "BHC":
						primaryAffiliation = "The Brooklyn Hospital Center";
						break;
					case "FMMP":
						primaryAffiliation = "Feto-Maternal Medical Polyclinic (Qatar)";
						break;
					case "HMRI":
						primaryAffiliation = "Houston Methodist Research Institute";
						break;
					case "JamaicaH":
						primaryAffiliation = "Jamaica Hospital";
						break;
					case "ANH":
						primaryAffiliation = "Amsterdam Nursing Home";
						break;
					case "Lenox":
						primaryAffiliation = "Lenox Hill Hospital";
						break;
					case "CU GHS":
						primaryAffiliation = "Cornell University Gannette Health Services";
						break;
					case "FlushHMC":
						primaryAffiliation = "Flushing Hospital Medical Center";
						break;
					case "AHP":
						primaryAffiliation = "American Hospital of Paris";
						break;
					case "LaGuardH":
						primaryAffiliation = "La Guardia Hospital";
						break;
					case "UGMA":
						primaryAffiliation = "University Group Medical Associates";
						break;
				
					default:
						primaryAffiliation = "Weill Cornell Medical College";
						break;
				}
			}
			ob.setRoles(modifiedRoles);
			log.info("Primary Affiliation for cwid " + cwid + " is " + primaryAffiliation);
			ob.setPrimaryAffiliation(primaryAffiliation);
			
			
			
			return ob;
				
				
		}
		
		/**
		 * <p>This function checks for tenure tracks in appointments and then merges them if found</p>
		 * @param roles list of role bean objects
		 * @return roles list of role bean objects
		 */
		private ArrayList<RoleBean> checkForTenureTrack(ArrayList<RoleBean> roles) {
			List<RoleBean> indexesToRemove = new ArrayList<>();
			for(int i=0 ; i < roles.size() -1 ; i++) {
				int index = i;
				for(int j = i+1; j < roles.size(); j++){
					if(roles.get(j).compareTo(roles.get(index))==1) {
						
						log.info("Found Tenure Track");
						log.info(roles.get(j).getDepartment() + " && " + roles.get(index).getDepartment() +  " :: " + roles.get(j).getTitleCode() + " && " + roles.get(index).getTitleCode() );
						if(Integer.parseInt(roles.get(j).getStartDate()) > Integer.parseInt(roles.get(index).getStartDate())){
							roles.get(j).setStartDate(roles.get(index).getStartDate());
							indexesToRemove.add(roles.get(index));
						} else if(Integer.parseInt(roles.get(j).getStartDate()) == Integer.parseInt(roles.get(index).getStartDate())) {
							if(roles.get(j).getEndDate() != null 
								&& 
								roles.get(j).getEndDate().equals("CURRENT")
								) {
								indexesToRemove.add(roles.get(index));
								break;
							}
							if(roles.get(index).getEndDate() != null 
								&& 
								roles.get(index).getEndDate().equals("CURRENT")
								) {
								indexesToRemove.add(roles.get(j));
								break;
							}
							
						} else {
							roles.get(index).setStartDate(roles.get(j).getStartDate());
							indexesToRemove.add(roles.get(j));
							break;
						}
						
						
					}
				}
			}
			roles.removeAll(indexesToRemove);
			
			for(RoleBean rb: roles) {
				log.info("Position - " + rb.toString());
			}
			return roles;
		}
		
		/**
		 * This function gets the department code from VIVO_DB
		 * @param deptName the department name for the grant
		 * @return the deptID
		 */
		private int getDepartmentCode(String deptName) {
			
			int deptId = 0;
			java.sql.ResultSet rs = null;
			Statement st = null;
			
			//log.info("Department Name in ED: " + deptName);
			
			/*if(deptName.trim().equals("Medicine")) {
				deptName ="Joan and Sanford I. Weill Department of Medicine";
			}
			
			if(deptName.trim().equals("Library")) {
				deptName ="Samuel J. Wood Library";
			}*/
			if(deptName.trim().equals("Otolaryngology - Head and Neck Surgery")) {
				deptName ="Otolaryngology - Head and Neck Surgery";
			}
			
			if(deptName.trim().equals("Otolaryngology")) {
				deptName ="Otolaryngology - Head and Neck Surgery";
			}
			
			/*if(deptName.trim().equals("Integrative Medicine")) {
				deptName ="Complementary and Integrative Medicine";
			}*/
					
			String selectQuery = "SELECT DISTINCT id FROM wcmc_department where TRIM(title) = '" + deptName.trim() + "'";
			
				try {
					st = this.con.createStatement();
					rs = st.executeQuery(selectQuery);
					if(rs!=null) { 
						rs.next(); 
						deptId = Integer.parseInt(rs.getString(1).trim());
					}
						
					}
					catch(SQLException sqle) {
						log.error("Exception:", sqle);
					}
					finally {
						try {
							if(rs != null)
								rs.close();
							if(st != null)
								st.close();
						} catch(SQLException e) {
							log.error("Error in closing connections:", e);
						}
					}
				
			return deptId;
					
		}
		
		/**
		 * The function gets education and training data from OFA
		 * @param cwid This is the institution wide unique identifier for a person
		 * @return List of education objects
		 */
		private ArrayList<EducationBean> getEducationAndTraining(String cwid) {
			
			ArrayList<EducationBean> edu = new ArrayList<EducationBean>();
			java.sql.ResultSet rs = null;
			Statement st = null;
			String selectQuery = "SELECT s.id,cwid, c.title, school_id, grad_year, n.title, degree_id \n" +
				    "from wcmc_person_school s \n" +
				    "LEFT JOIN wcmc_person p \n" +
				      "ON p.id = s.person_id \n" +
				    "LEFT JOIN wcmc_school_degree n \n" +
				      "ON n.id = s.degree_id \n" +
				    "LEFT JOIN wcmc_school c \n" +
				      "ON c.id = s.school_id \n" +
				    "WHERE s.degree_id is NOT NULL AND s.grad_year IS NOT NULL and c.title IS NOT NULL and cwid is not null and cwid= '" + cwid + "'";
			
			//log.info(selectQuery);
			
			
				try {
					st = this.con.createStatement();
					rs = st.executeQuery(selectQuery);
					if(rs!=null) {
						
						while(rs.next()) {
							EducationBean ebean = new EducationBean();
							ebean.setDegreePk(rs.getString(1));
							ebean.setDateTimeInterval(rs.getString(5));
							ebean.setInstituteFk(rs.getString(4));
							ebean.setDegreeName(StringEscapeUtils.escapeJava(rs.getString(6)));
							ebean.setInstituion(StringEscapeUtils.escapeJava(rs.getString(3)));
							ebean.setBuiltInDegreePk(rs.getString(7));
							//log.info(ebean.toString());
							edu.add(ebean);
						}
					}
				}
				catch(SQLException sqle) {
					log.error("Exception:", sqle);
				}
				finally {
					try {
						if(rs != null)
							rs.close();
						if(st != null)
							st.close();
					} catch(SQLException e) {
						// TODO Auto-generated catch block
						log.error("Error in closing connections:", e);
					}
				}
			
			return edu;
		}
		
		/**
		 * This function inserts OFA data into VIVO to wcmcOfa graph
		 * @param ob The bean object containing role and education & training data both
		 */
		private void insertOfaDataInVivo(OfaBean ob) {
			Date now = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			String strDate = sdf.format(now);
			
			StringBuilder sb = new StringBuilder();
			sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
			sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
			sb.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
			sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
			sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
			sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
			sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
			sb.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> { \n");
			for(RoleBean rb: ob.getRoles()) {
				if(rb.isInterimAppointment()) {
					if(ob.getRoles().stream().anyMatch(role -> role.isActiveAppointment() == true)) {
						log.info("Skipping interim appointment " + rb.getTitleCode());
					} else {
						rb.setCrudStatus("INSERT");
						sb.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> . \n");
						if(rb.isPrimaryAppointment())
							sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> rdf:type core:PrimaryPosition . \n");
						
						sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> rdf:type core:Position . \n");
						sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> rdf:type core:Relationship . \n");
						sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> rdf:type obo:BFO_0000002 . \n");
						sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> rdf:type obo:BFO_0000001 . \n");
						sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> rdf:type obo:BFO_0000020 . \n");
						sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> rdfs:label \"" + rb.getTitleCode().trim() + "\" . \n");
						sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> core:relates <" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> . \n");
						sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> core:DateTimeValue \"" + strDate + "\" . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> core:relatedBy <" + this.vivoNamespace + "position-" + rb.getSorId().trim() +"> . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> rdf:type core:Department . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> rdf:type core:AcademicDepartment . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> vitro:mostSpecificType core:AcademicDepartment . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> rdfs:label \"" + rb.getDepartment() + "\" . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> <http://purl.obolibrary.org/obo/BFO_0000050> <http://vivo.med.cornell.edu/individual/org-568> . \n");
						sb.append("<http://vivo.med.cornell.edu/individual/org-568> rdf:type core:University . \n");
						sb.append("<http://vivo.med.cornell.edu/individual/org-568> rdfs:label \"Weill Cornell Medical College\" . \n");
						sb.append("<http://vivo.med.cornell.edu/individual/org-568> <http://purl.obolibrary.org/obo/BFO_0000051> <" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> . \n");
						
						//if there is end date 
						if(rb.getEndDate() != null && !rb.getEndDate().equals("CURRENT")) {
							//For Date Time Interval
								sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to" + rb.getEndDate().trim() + "> . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to" + rb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to" + rb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to" + rb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-" + rb.getEndDate().trim() + "> . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to" + rb.getEndDate().trim() + "> vitro:mostSpecificType core:DateTimeInterval . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to" + rb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
								//For Start Date
								sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> core:dateTime \"" + rb.getStartDate().trim() + "\" .\n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> core:dateTime \"" + rb.getStartDate().trim() + "-01-01T00:00:00\" .\n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> vitro:mostSpecificType core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
								//For End Date
								sb.append("<" + this.vivoNamespace + "date-" + rb.getEndDate().trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getEndDate().trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getEndDate().trim() + "> core:dateTime \"" + rb.getEndDate().trim() + "\" .\n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getEndDate().trim() + "> core:dateTime \"" + rb.getEndDate().trim() + "-01-01T00:00:00\" .\n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getEndDate().trim() + "> vitro:mostSpecificType core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						}
						//if there is no end date
						else {
								sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to> . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to> rdf:type core:DateTimeInterval . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to> core:start <" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to> vitro:mostSpecificType core:DateTimeInterval . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> core:dateTime \"" + rb.getStartDate().trim() + "\" .\n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> core:dateTime \"" + rb.getStartDate().trim() + "-01-01T00:00:00\" .\n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> vitro:mostSpecificType core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						}
						sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
					}
					
				} else {
					rb.setCrudStatus("INSERT");
					sb.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> . \n");
					if(rb.isPrimaryAppointment())
						sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> rdf:type core:PrimaryPosition . \n");
					
					sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> rdf:type core:Position . \n");
					sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> rdf:type core:Relationship . \n");
					sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> rdf:type obo:BFO_0000002 . \n");
					sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> rdf:type obo:BFO_0000001 . \n");
					sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> rdf:type obo:BFO_0000020 . \n");
					sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> rdfs:label \"" + rb.getTitleCode().trim() + "\" . \n");
					sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> . \n");
					sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> core:relates <" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> . \n");
					sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> core:DateTimeValue \"" + strDate + "\" . \n");
					sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> core:relatedBy <" + this.vivoNamespace + "position-" + rb.getSorId().trim() +"> . \n");
					sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> rdf:type core:Department . \n");
					sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> rdf:type core:AcademicDepartment . \n");
					sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> vitro:mostSpecificType core:AcademicDepartment . \n");
					sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> rdfs:label \"" + rb.getDepartment() + "\" . \n");
					sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> <http://purl.obolibrary.org/obo/BFO_0000050> <http://vivo.med.cornell.edu/individual/org-568> . \n");
					sb.append("<http://vivo.med.cornell.edu/individual/org-568> rdf:type core:University . \n");
					sb.append("<http://vivo.med.cornell.edu/individual/org-568> rdfs:label \"Weill Cornell Medical College\" . \n");
					sb.append("<http://vivo.med.cornell.edu/individual/org-568> <http://purl.obolibrary.org/obo/BFO_0000051> <" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> . \n");
					
					//if there is end date 
					if(rb.getEndDate() != null && !rb.getEndDate().equals("CURRENT")) {
						//For Date Time Interval
							sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to" + rb.getEndDate().trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to" + rb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to" + rb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to" + rb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-" + rb.getEndDate().trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to" + rb.getEndDate().trim() + "> vitro:mostSpecificType core:DateTimeInterval . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to" + rb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							//For Start Date
							sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> rdf:type core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> core:dateTimePrecision core:yearPrecision . \n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> core:dateTime \"" + rb.getStartDate().trim() + "\" .\n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> core:dateTime \"" + rb.getStartDate().trim() + "-01-01T00:00:00\" .\n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> vitro:mostSpecificType core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							//For End Date
							sb.append("<" + this.vivoNamespace + "date-" + rb.getEndDate().trim() + "> rdf:type core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getEndDate().trim() + "> core:dateTimePrecision core:yearPrecision . \n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getEndDate().trim() + "> core:dateTime \"" + rb.getEndDate().trim() + "\" .\n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getEndDate().trim() + "> core:dateTime \"" + rb.getEndDate().trim() + "-01-01T00:00:00\" .\n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getEndDate().trim() + "> vitro:mostSpecificType core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
					}
					//if there is no end date
					else {
							sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to> rdf:type core:DateTimeInterval . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to> core:start <" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to> vitro:mostSpecificType core:DateTimeInterval . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-" + rb.getStartDate().trim() + "to> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> rdf:type core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> core:dateTimePrecision core:yearPrecision . \n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> core:dateTime \"" + rb.getStartDate().trim() + "\" .\n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> core:dateTime \"" + rb.getStartDate().trim() + "-01-01T00:00:00\" .\n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> vitro:mostSpecificType core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-" + rb.getStartDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
					}
					sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
				}
			}
			//For education and background
			for(EducationBean edu:ob.getEdu()) {
				//for educational training
				edu.setCrudStatus("INSERT");
				sb.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> . \n");
				sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> rdf:type core:AwardedDegree . \n");
				sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> rdfs:label \"" + edu.getDegreeName().trim() + "\" . \n");
				sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:relates <" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> . \n");
				sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> . \n");
				sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:assignedBy <" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> .\n");
				sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0002353 <" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> .\n");
				sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:DateTimeValue \"" + strDate + "\" .\n");
				sb.append("<" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> <http://vivoweb.org/ontology/core#abbreviation> \"" + edu.getDegreeName().trim() + "\" . \n");
				sb.append("<" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> rdfs:label \"" + edu.getDegreeName().trim() + "\" . \n");
				sb.append("<" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> core:relatedBy <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> . \n");
				sb.append("<" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> rdf:type <http://vivoweb.org/ontology/core#AcademicDegree> . \n");
				sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> rdf:type <http://xmlns.com/foaf/0.1/Organization> .\n");
				sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
				sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> rdfs:label \"" + edu.getInstituion() + "\" . \n");
				sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> core:assigns <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> .\n");
				sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
				
				//for educational process
				sb.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> . \n");
				sb.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> rdf:type core:EducationalProcess . \n");
				sb.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0000057 <" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> . \n");
				sb.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0000057 <" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> . \n");
				sb.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0002234 <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> . \n");
				if(!ob.getCwid().equalsIgnoreCase("jis2011")) {
					sb.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + edu.getDateTimeInterval().trim() + "> . \n");
					sb.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:DateTimeValue \"" + strDate + "\" .\n");
					sb.append("<" + this.vivoNamespace + "dtinterval-" + edu.getDateTimeInterval().trim() + "> rdf:type core:DateTimeInterval . \n");
					sb.append("<" + this.vivoNamespace + "dtinterval-" + edu.getDateTimeInterval().trim() + "> core:end <" + this.vivoNamespace + "date-" + edu.getDateTimeInterval().substring(2).trim() + "> . \n");
					sb.append("<" + this.vivoNamespace + "date-" + edu.getDateTimeInterval().substring(2).trim() + "> rdf:type core:DateTimeValue . \n");
					sb.append("<" + this.vivoNamespace + "date-" + edu.getDateTimeInterval().substring(2).trim() + "> core:dateTimePrecision core:yearPrecision . \n");
					sb.append("<" + this.vivoNamespace + "date-" + edu.getDateTimeInterval().substring(2).trim() + "> core:dateTime \"" + edu.getDateTimeInterval().trim() + "\" . \n");
				}
				sb.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
				
				
			}
			sb.append("}}");
			
			
			//log.info(sb.toString());
			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				try{
					String response = this.vivoClient.vivoUpdateApi(sb.toString());
					log.info(response);
				} catch(Exception  e) {
					log.info("Api Exception", e);
				}
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				try {
					SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
					runSparqlUpdateTemplate(sb.toString(), vivoJena);
					if(vivoJena != null)
						this.jcf.returnConnectionToPool(vivoJena, "dataSet");
				} catch(IOException e) {
					log.error("Exception in connecting to Jena" ,e);
				}
			} else {
				try {
					TDBJenaConnect vivoJena = this.tcf.getConnectionfromPool("dataSet");
					runTDBSparqlUpdateTemplate(sb.toString(), vivoJena);
					if(vivoJena != null)
						this.tcf.returnConnectionToPool(vivoJena, "dataSet");
				} catch(IOException e) {
					log.error("Exception in connecting to Jena" ,e);
				}
			}
			
			insertInferenceTriples(ob);
		}
		
		/**
		 * This function insert inference triples based on operation 
		 * @param ob The bean object containing role and education & training data both
		 */
		private void insertInferenceTriples(OfaBean ob) {
			
			int insertCount = 0 ;
			StringBuilder sb = new StringBuilder();
			sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
			sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
			sb.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
			sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
			sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
			sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
			sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
			sb.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> { \n");
			//For education and background
			if(ob.getEdu() != null && !ob.getEdu().isEmpty()) {
				for(EducationBean edu:ob.getEdu()) {
					if(edu.getCrudStatus() != null && edu.getCrudStatus().equals("INSERT")) {
						sb.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> obo:RO_0000056 <" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> . \n");
						sb.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> core:relates <" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
						sb.append("<" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> rdf:type <http://www.w3.org/2004/02/skos/core#Concept> . \n");
						sb.append("<" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> vitro:mostSpecificType <http://vivoweb.org/ontology/core#AcademicDegree> . \n");
						insertCount = insertCount + 1;
					}
					if(edu.getCrudStatus() != null && edu.getCrudStatus().equals("UPDATE")) {
						sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> obo:RO_0000056 <" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> . \n");
						sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> rdf:type obo:BFO_0000004 . \n");
						sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> rdf:type obo:BFO_0000001 . \n");
						sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> rdf:type obo:BFO_0000002 . \n");
						sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> rdf:type <http://xmlns.com/foaf/0.1/Organization> . \n");
						sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
						sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> rdf:type <http://xmlns.com/foaf/0.1/Agent> . \n");
					}
				}
			}
			//For roles
			if(ob.getRoles() != null && !ob.getRoles().isEmpty()) {
				for(RoleBean rb: ob.getRoles()) {
					if(rb.getCrudStatus() != null && rb.getCrudStatus().equals("INSERT")) {
						sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() +"> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
						sb.append("<" + this.vivoNamespace + "position-" + rb.getSorId().trim() +"> vitro:mostSpecificType core:Position . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> core:contributingRole <" + this.vivoNamespace + "position-" + rb.getSorId().trim() +"> . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> rdf:type obo:BFO_0000004 . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> rdf:type obo:BFO_0000001 . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> rdf:type obo:BFO_0000002 . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> rdf:type <http://xmlns.com/foaf/0.1/Organization> . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> rdf:type <http://xmlns.com/foaf/0.1/Agent> . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + rb.getDeptCode() + "> vitro:mostSpecificType core:AcademicDepartment . \n");
						sb.append("<" + this.vivoNamespace + "org-568" + rb.getDeptCode() + "> rdf:type obo:BFO_0000004 . \n");
						sb.append("<" + this.vivoNamespace + "org-568" + rb.getDeptCode() + "> rdf:type obo:BFO_0000001 . \n");
						sb.append("<" + this.vivoNamespace + "org-568" + rb.getDeptCode() + "> rdf:type obo:BFO_0000002 . \n");
						sb.append("<" + this.vivoNamespace + "org-568" + rb.getDeptCode() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
						sb.append("<" + this.vivoNamespace + "org-568" + rb.getDeptCode() + "> rdf:type <http://xmlns.com/foaf/0.1/Agent> . \n");
						sb.append("<" + this.vivoNamespace + "org-568" + rb.getDeptCode() + "> vitro:mostSpecificType core:University . \n");
						insertCount = insertCount + 1;
					}
				}
			}
			sb.append("}}");
			if(insertCount > 0 ) {
				log.info("Inserting inference triples for " + ob.getCwid());
				if(ingestType.equals(IngestType.VIVO_API.toString())) {
					try{
						String response = this.vivoClient.vivoUpdateApi(sb.toString());
						log.info(response);
					} catch(Exception  e) {
						log.info("Api Exception", e);
					}
				} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
					try {
						SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
						runSparqlUpdateTemplate(sb.toString(), vivoJena);
						if(vivoJena != null)
							this.jcf.returnConnectionToPool(vivoJena, "dataSet");
					} catch(IOException e) {
						log.error("Exception in connecting to Jena" ,e);
					}
				} else {
					try {
						TDBJenaConnect vivoJena = this.tcf.getConnectionfromPool("dataSet");
						runTDBSparqlUpdateTemplate(sb.toString(), vivoJena);
						if(vivoJena != null)
							this.tcf.returnConnectionToPool(vivoJena, "dataSet");
					} catch(IOException e) {
						log.error("Exception in connecting to Jena" ,e);
					}
				}
			}
			else
				log.info("No inference triples to insert");
		}
		
		/**
		 * This function check for updates against existing data in VIVO. If there is any change in data such as end date for a position, a new position, addition of education and training.
		 * The data is automatically updated and inferenced.
		 * @param ob The bean object containing role and education & training data both
		 * @param cwid The unqiue identifier for a person
		 * @return Update count
		 * @throws IOException thrown by SDBJenaConnect
		 */
		private int checkForUpdates(OfaBean ob, String cwid) throws IOException {
			Date now = new Date();
			String currYear = new SimpleDateFormat("yyyy").format(new Date());
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			String strDate = sdf.format(now);
			int updateCount = 0;
			
			ArrayList<RoleBean> rb = ob.getRoles();
			ArrayList<EducationBean> ebean = ob.getEdu();
			

			//Checking for appointment updates
			for(RoleBean role: rb) {
				StringBuffer sb = new StringBuffer();
				sb.append("SELECT ?obj \n");
				sb.append("WHERE {\n");
				sb.append("GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> {\n");
				sb.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> ?obj . \n");
				sb.append("}}");
				
				TDBJenaConnect vivoJena = this.tcf.getConnectionfromPool("dataSet");
				
				ResultSet rs = runTDBSparqlTemplate(sb.toString(), vivoJena);
				
				
				if(!rs.hasNext()) {
					if(!role.isInterimAppointment()) {
						//insert
						log.info("Insert new appointment - position-" + role.getSorId().trim());
						
						role.setCrudStatus("INSERT");
						StringBuffer insertQuery = new StringBuffer();
						insertQuery.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						insertQuery.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
						insertQuery.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
						insertQuery.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
						insertQuery.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
						insertQuery.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
						insertQuery.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
						insertQuery.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> { \n");
						insertQuery.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> . \n");
						if(role.isPrimaryAppointment())
							insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> rdf:type core:PrimaryPosition . \n");
							
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> rdf:type core:Position . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> rdf:type core:Relationship . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> rdf:type obo:BFO_0000002 . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> rdf:type obo:BFO_0000001 . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> rdf:type obo:BFO_0000020 . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> rdfs:label \"" + role.getTitleCode().trim() + "\" . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:relates <" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:DateTimeValue \"" + strDate + "\" . \n");
						insertQuery.append("<" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> core:relatedBy <" + this.vivoNamespace + "position-" + role.getSorId().trim() +"> . \n");
						insertQuery.append("<" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> rdf:type core:AcademicDepartment . \n");
						insertQuery.append("<" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> vitro:mostSpecificType core:AcademicDepartment . \n");
						insertQuery.append("<" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> rdf:type core:Department . \n");
						insertQuery.append("<" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> rdfs:label \"" + role.getDepartment() + "\" . \n");
						insertQuery.append("<" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> <http://purl.obolibrary.org/obo/BFO_0000050> <http://vivo.med.cornell.edu/individual/org-568> . \n");
						insertQuery.append("<http://vivo.med.cornell.edu/individual/org-568> rdf:type core:University . \n");
						insertQuery.append("<http://vivo.med.cornell.edu/individual/org-568> rdfs:label \"Weill Cornell Medical College\" . \n");
						insertQuery.append("<http://vivo.med.cornell.edu/individual/org-568> <http://purl.obolibrary.org/obo/BFO_0000051> <" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> . \n");
							//if there is end date 
							if(role.getEndDate() != null && !role.getEndDate().equals("CURRENT")) {
								//For Date Time Interval
								insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> vitro:mostSpecificType core:DateTimeInterval . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
									//For Start Date
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> rdf:type core:DateTimeValue . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> core:dateTime \"" + role.getStartDate().trim() + "\" .\n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> core:dateTime \"" + role.getStartDate().trim() + "-01-01T00:00:00\" .\n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> vitro:mostSpecificType core:DateTimeValue . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
									//For End Date
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> rdf:type core:DateTimeValue . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> core:dateTime \"" + role.getEndDate().trim() + "\" .\n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> core:dateTime \"" + role.getEndDate().trim() + "-01-01T00:00:00\" .\n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> vitro:mostSpecificType core:DateTimeValue . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							//if there is no end date
							else {
								insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> rdf:type core:DateTimeInterval . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> core:start <" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> vitro:mostSpecificType core:DateTimeInterval . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> rdf:type core:DateTimeValue . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> core:dateTime \"" + role.getStartDate().trim() + "\" .\n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> core:dateTime \"" + role.getStartDate().trim() + "-01-01T00:00:00\" .\n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> vitro:mostSpecificType core:DateTimeValue . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						insertQuery.append("}}");
						
						try {
							
							runTDBSparqlUpdateTemplate(insertQuery.toString(), vivoJena);
							
							
						} catch(IOException e) {
							// TODO Auto-generated catch block
							log.error("IOException", e);
						}
					}
				}
				
				else
				{
					
					
					QuerySolution qs = rs.nextSolution();
					
					
					String endDate = qs.get("obj").toString().replace(this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to", "");
					role.setCrudStatus("UPDATE");
					
					//Update an appoinment to have an end date
					if(endDate.length() == 0 && role.getEndDate() != null && !role.getEndDate().equals("CURRENT")) {
						log.info("Update existing appointment position-" + role.getSorId().trim());
						StringBuffer updateQuery = new StringBuffer();
						updateQuery.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						updateQuery.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n"); 
						updateQuery.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
						updateQuery.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n");
						updateQuery.append("DELETE { \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + qs.get("obj").toString() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#DateTimeValue> ?date . \n");
						//updateQuery.append("<" + qs.get("obj").toString() + "> ?p ?o . \n");
						updateQuery.append("} \n");
						updateQuery.append("INSERT { \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:DateTimeValue \"" + strDate + "\" . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> vitro:mostSpecificType core:DateTimeInterval . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						updateQuery.append("} \n");
						updateQuery.append("WHERE { \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + qs.get("obj").toString() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#DateTimeValue> ?date . \n");
						//updateQuery.append("<" + qs.get("obj").toString() + "> ?p ?o . \n");
						updateQuery.append("}");
						
						log.info(updateQuery.toString());
						
						try {
							
							runTDBSparqlUpdateTemplate(updateQuery.toString(), vivoJena);
							
						} catch(IOException e) {
							// TODO Auto-generated catch block
							log.error("IOException" ,e);
						}
						
						updateCount = updateCount + 1;
						
					}
					//Update to delete any end date which is for a current appointment
					else if(endDate.equals(currYear) && role.getEndDate().equals("CURRENT")) {
						log.info("Update existing appointment position-" + role.getSorId().trim());
						StringBuffer updateQuery = new StringBuffer();
						updateQuery.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						updateQuery.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n"); 
						updateQuery.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
						updateQuery.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n");
						updateQuery.append("DELETE { \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + qs.get("obj").toString() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#DateTimeValue> ?date . \n");
						//updateQuery.append("<" + qs.get("obj").toString() + "> ?p ?o . \n");
						updateQuery.append("} \n");
						updateQuery.append("INSERT { \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:DateTimeValue \"" + strDate + "\" . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> rdf:type core:DateTimeInterval . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> core:start <" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> vitro:mostSpecificType core:DateTimeInterval . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						updateQuery.append("} \n");
						updateQuery.append("WHERE { \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + qs.get("obj").toString() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#DateTimeValue> ?date . \n");
						//updateQuery.append("OPTIONAL {<" + qs.get("obj").toString() + "> ?p ?o .} \n");
						updateQuery.append("}");
						log.info(updateQuery.toString());
						
						
						try {
							
							runTDBSparqlUpdateTemplate(updateQuery.toString(), vivoJena);
							
						} catch(IOException e) {
							// TODO Auto-generated catch block
							log.error("IOException" ,e);
						}
						
						updateCount = updateCount + 1;
					}
					//Delete interim appointment
					else if(role.isInterimAppointment()) {
						StringBuffer updateQuery = new StringBuffer();
						updateQuery.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						updateQuery.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n"); 
						updateQuery.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
						updateQuery.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n");
						updateQuery.append("DELETE { \n");
						updateQuery.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> ?p ?o . \n");
						updateQuery.append("} \n");
						updateQuery.append("WHERE { \n");
						updateQuery.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> ?p ?o . \n");
						updateQuery.append("}");
						log.info(updateQuery.toString());
						
						
						try {
							
							runTDBSparqlUpdateTemplate(updateQuery.toString(), vivoJena);
							
						} catch(IOException e) {
							log.error("IOException" ,e);
						}
						
						updateCount = updateCount + 1;
					}
					else
						log.info("No updates are necessary for " + ob.getCwid().trim() + " for position-" + role.getSorId().trim());
				}
				//Close the connection
				if(vivoJena != null)
					this.tcf.returnConnectionToPool(vivoJena, "dataSet");
			}
			//Checking for education and training updates
			for(EducationBean edu: ebean) {
				StringBuilder sb = new StringBuilder();
				
				sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
				sb.append("SELECT ?obj \n");
				sb.append("WHERE { \n");
				sb.append("GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> {\n");
				sb.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + ">");
				sb.append("}}");
				
				//log.info(sb.toString());
				
				TDBJenaConnect vivoJena = this.tcf.getConnectionfromPool("dataSet");
				
				ResultSet rs = runTDBSparqlTemplate(sb.toString(), vivoJena);
				
				
				if(rs != null && !rs.hasNext()) {
					log.info("Insert new education for " + ob.getCwid().trim() + " - educationalTraining-" + edu.getDegreePk().trim());
					edu.setCrudStatus("INSERT");
					StringBuffer insertQuery = new StringBuffer();
					insertQuery.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
					insertQuery.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
					insertQuery.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
					insertQuery.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
					insertQuery.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
					insertQuery.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
					insertQuery.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
					insertQuery.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> { \n");
					//for educational training
					insertQuery.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> rdf:type core:AwardedDegree . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> rdfs:label \"" + edu.getDegreeName().trim() + "\" . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:relates <" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:assignedBy <" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> .\n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0002353 <" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> .\n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:DateTimeValue \"" + strDate + "\" .\n");
					insertQuery.append("<" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> <http://vivoweb.org/ontology/core#abbreviation> \"" + edu.getDegreeName().trim() + "\" . \n");
					insertQuery.append("<" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> rdfs:label \"" + edu.getDegreeName().trim() + "\" . \n");
					insertQuery.append("<" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> core:relatedBy <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> rdf:type <http://vivoweb.org/ontology/core#AcademicDegree> . \n");
					insertQuery.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> rdf:type <http://xmlns.com/foaf/0.1/Organization> .\n");
					insertQuery.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
					insertQuery.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> core:assigns <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> .\n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
					
					//for educational process
					insertQuery.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> rdf:type core:EducationalProcess . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0000057 <" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0000057 <" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0002234 <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + edu.getDateTimeInterval().trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:DateTimeValue \"" + strDate + "\" .\n");
					insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + edu.getDateTimeInterval().trim() + "> rdf:type core:DateTimeInterval . \n");
					insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + edu.getDateTimeInterval().trim() + "> core:end <" + this.vivoNamespace + "date-" + edu.getDateTimeInterval().substring(2).trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "date-" + edu.getDateTimeInterval().substring(2).trim() + "> rdf:type core:DateTimeValue . \n");
					insertQuery.append("<" + this.vivoNamespace + "date-" + edu.getDateTimeInterval().substring(2).trim() + "> core:dateTimePrecision core:yearPrecision . \n");
					insertQuery.append("<" + this.vivoNamespace + "date-" + edu.getDateTimeInterval().substring(2).trim() + "> core:dateTime \"" + edu.getDateTimeInterval().substring(2).trim() + "\" . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
					insertQuery.append("}}");
					
					log.info(insertQuery.toString());
					
					try {
						
						runTDBSparqlUpdateTemplate(insertQuery.toString(), vivoJena);
						
					} catch(IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					updateCount = updateCount + 1;
					
				}
				else {
					//Check for change of institution
					
					edu.setCrudStatus("UPDATE");
					int instituteFk = 0;
					String institutionLabel = null;
					sb.setLength(0);
					sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>");
					sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
					sb.append("SELECT ?instituteFk ?institutionLabel\n");
					sb.append("WHERE { \n");
					sb.append("GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> {\n");
					sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> core:assignedBy ?instituteFk . \n");
					sb.append("?instituteFk rdfs:label ?institutionLabel . ");
					sb.append("}}");
					
					rs = runTDBSparqlTemplate(sb.toString(), vivoJena);
					
					if(rs.hasNext()) {
						QuerySolution qs = rs.nextSolution();
						instituteFk = Integer.parseInt(qs.get("instituteFk").toString().replace(this.vivoNamespace + "org-", ""));
						institutionLabel = qs.get("institutionLabel").toString();
					}
					if(instituteFk != Integer.parseInt(edu.getInstituteFk())) {
						log.info("Insitition needs to be updated to " + edu.getInstituion() + " for educationalTraining-" + edu.getDegreePk().trim() + " with cwid " + ob.getCwid().trim());
						sb.setLength(0);
						sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>");
						sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
						sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
						sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n");
						sb.append("DELETE { \n");
						sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> core:assignedBy <" + this.vivoNamespace + "org-" + instituteFk + "> . \n");
						sb.append("<" + this.vivoNamespace + "org-" + instituteFk + "> core:assigns <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> .\n");
						sb.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0000057 <" + this.vivoNamespace + "org-" + instituteFk + "> . \n");
						sb.append("} \n");
						sb.append("INSERT { \n");
						sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> core:assignedBy <" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> core:assigns <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> .\n");
						sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> rdf:type <http://xmlns.com/foaf/0.1/Organization> .\n");
						sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> rdfs:label \"" + edu.getInstituion() + "\" .\n");
						sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" .\n");
						sb.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0000057 <" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> . \n");
						sb.append("} \n");
						sb.append("WHERE { \n");
						sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> core:assignedBy <" + this.vivoNamespace + "org-" + instituteFk + "> . \n");
						sb.append("<" + this.vivoNamespace + "org-" + instituteFk + "> core:assigns <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> .\n");
						sb.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0000057 <" + this.vivoNamespace + "org-" + instituteFk + "> . \n");
						sb.append("}");
						
						//log.info(sb.toString());
						try {
							runTDBSparqlUpdateTemplate(sb.toString(), vivoJena);
						} catch(IOException e) {
							log.error("IOException" ,e);
						}
					} 
					else	
						log.info("No updates are necessary for " + ob.getCwid().trim() + " for educationalTraining-" + edu.getDegreePk().trim());
					
					//Check for insitution name 
					if(institutionLabel != null && !institutionLabel.equalsIgnoreCase(edu.getInstituion())) {
						log.info("Insitition Label needs to be updated to " + edu.getInstituion() + " for educationalTraining-" + edu.getDegreePk().trim() + " with cwid " + ob.getCwid().trim());
						sb.setLength(0);
						sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>");
						sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
						sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
						sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n");
						sb.append("DELETE { \n");
						sb.append("<" + this.vivoNamespace + "org-" + instituteFk + "> rdfs:label ?label .\n");
						sb.append("} \n");
						sb.append("INSERT { \n");
						sb.append("<" + this.vivoNamespace + "org-" + instituteFk + "> rdfs:label \"" + edu.getInstituion() + "\" .\n");
						sb.append("} \n");
						sb.append("WHERE { \n");
						sb.append("<" + this.vivoNamespace + "org-" + instituteFk + "> rdfs:label ?label .\n");
						sb.append("}");
						
						//log.info(sb.toString());
						try {
							runTDBSparqlUpdateTemplate(sb.toString(), vivoJena);
						} catch(IOException e) {
							log.error("IOException" ,e);
						}
					}
					else	
						log.info("No updates are necessary for institution name " + ob.getCwid().trim() + " for educationalTraining-" + edu.getDegreePk().trim());
				}
				//Close the connection
				if(vivoJena != null)
					this.tcf.returnConnectionToPool(vivoJena, "dataSet");
				
			}
			
			
			insertInferenceTriples(ob);
			//Check if any appointment needs to be deleted in VIVO
			syncAppointmentsInVivo(rb, cwid);
			
			return updateCount;
			
		}

		/**
		 * This function check for updates against existing data in VIVO. If there is any change in data such as end date for a position, a new position, addition of education and training.
		 * The data is automatically updated and inferenced.
		 * @param ob The bean object containing role and education & training data both
		 * @param cwid The unqiue identifier for a person
		 * @return Update count
		 * @throws IOException thrown by SDBJenaConnect
		 */
		private int checkForUpdatesUsingTDB(OfaBean ob, String cwid) throws IOException {
			Date now = new Date();
			String strDate = sdf.format(now);
			int updateCount = 0;
			
			ArrayList<RoleBean> rb = ob.getRoles();
			ArrayList<EducationBean> ebean = ob.getEdu();
			

			//Checking for appointment updates
			for(RoleBean role: rb) {
				StringBuffer sb = new StringBuffer();
				sb.append("SELECT ?obj \n");
				sb.append("WHERE {\n");
				sb.append("GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> { \n");
				sb.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> ?obj . \n");
				sb.append("}}");

				try {
					String response = this.vivoClient.vivoQueryApi(sb.toString());
					log.info(response);
					JSONObject obj = new JSONObject(response);
					JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
				if(bindings.isEmpty()) {
					if(!role.isInterimAppointment()) {
						//insert
						log.info("Insert new appointment - position-" + role.getSorId().trim());
						
						role.setCrudStatus("INSERT");
						StringBuffer insertQuery = new StringBuffer();
						insertQuery.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						insertQuery.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
						insertQuery.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
						insertQuery.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
						insertQuery.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
						insertQuery.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
						insertQuery.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
						insertQuery.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> { \n");
						insertQuery.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> . \n");
						if(role.isPrimaryAppointment())
							insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> rdf:type core:PrimaryPosition . \n");
							
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> rdf:type core:Position . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> rdf:type core:Relationship . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> rdf:type obo:BFO_0000002 . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> rdf:type obo:BFO_0000001 . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> rdf:type obo:BFO_0000020 . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> rdfs:label \"" + role.getTitleCode().trim() + "\" . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:relates <" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> . \n");
						insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:DateTimeValue \"" + strDate + "\" . \n");
						insertQuery.append("<" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> core:relatedBy <" + this.vivoNamespace + "position-" + role.getSorId().trim() +"> . \n");
						insertQuery.append("<" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> rdf:type core:AcademicDepartment . \n");
						insertQuery.append("<" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> vitro:mostSpecificType core:AcademicDepartment . \n");
						insertQuery.append("<" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> rdf:type core:Department . \n");
						insertQuery.append("<" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> rdfs:label \"" + role.getDepartment() + "\" . \n");
						insertQuery.append("<" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> <http://purl.obolibrary.org/obo/BFO_0000050> <http://vivo.med.cornell.edu/individual/org-568> . \n");
						insertQuery.append("<http://vivo.med.cornell.edu/individual/org-568> rdf:type core:University . \n");
						insertQuery.append("<http://vivo.med.cornell.edu/individual/org-568> rdfs:label \"Weill Cornell Medical College\" . \n");
						insertQuery.append("<http://vivo.med.cornell.edu/individual/org-568> <http://purl.obolibrary.org/obo/BFO_0000051> <" + this.vivoNamespace + "org-u" + role.getDeptCode() + "> . \n");
							//if there is end date 
							if(role.getEndDate() != null && !role.getEndDate().equals("CURRENT")) {
								//For Date Time Interval
								insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> vitro:mostSpecificType core:DateTimeInterval . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
									//For Start Date
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> rdf:type core:DateTimeValue . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> core:dateTime \"" + role.getStartDate().trim() + "\" .\n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> core:dateTime \"" + role.getStartDate().trim() + "-01-01T00:00:00\" .\n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> vitro:mostSpecificType core:DateTimeValue . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
									//For End Date
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> rdf:type core:DateTimeValue . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> core:dateTime \"" + role.getEndDate().trim() + "\" .\n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> core:dateTime \"" + role.getEndDate().trim() + "-01-01T00:00:00\" .\n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> vitro:mostSpecificType core:DateTimeValue . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							//if there is no end date
							else {
								insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> rdf:type core:DateTimeInterval . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> core:start <" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> vitro:mostSpecificType core:DateTimeInterval . \n");
								insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> rdf:type core:DateTimeValue . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> core:dateTime \"" + role.getStartDate().trim() + "\" .\n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> core:dateTime \"" + role.getStartDate().trim() + "-01-01T00:00:00\" .\n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> vitro:mostSpecificType core:DateTimeValue . \n");
								insertQuery.append("<" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							insertQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						insertQuery.append("}}");
						response = this.vivoClient.vivoUpdateApi(insertQuery.toString());
						log.info(response);
					}
				}
				
				else
				{
					String endDate = bindings.getJSONObject(0).optJSONObject("obj").getString("value").replace(this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to", "");
					String dateTimeInterval = bindings.getJSONObject(0).optJSONObject("obj").getString("value");
					role.setCrudStatus("UPDATE");
					
					//Update an appoinment to have an end date
					if(endDate.length() == 0 && role.getEndDate() != null && !role.getEndDate().equals("CURRENT")) {
						log.info("Update existing appointment position-" + role.getSorId().trim());
						StringBuffer updateQuery = new StringBuffer();
						updateQuery.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						updateQuery.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n"); 
						updateQuery.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
						updateQuery.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n");
						updateQuery.append("DELETE { \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + dateTimeInterval + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#DateTimeValue> ?date . \n");
						//updateQuery.append("<" + qs.get("obj").toString() + "> ?p ?o . \n");
						updateQuery.append("} \n");
						updateQuery.append("INSERT { \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:DateTimeValue \"" + strDate + "\" . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> vitro:mostSpecificType core:DateTimeInterval . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						updateQuery.append("} \n");
						updateQuery.append("WHERE { \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + dateTimeInterval + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#DateTimeValue> ?date . \n");
						//updateQuery.append("<" + qs.get("obj").toString() + "> ?p ?o . \n");
						updateQuery.append("}");
						
						log.info(updateQuery.toString());
						response = this.vivoClient.vivoUpdateApi(updateQuery.toString());
						log.info(response);
						
						updateCount = updateCount + 1;
						
					}
					//Update to delete any end date which is for a current appointment
					else if(endDate.equals(this.currYear) && role.getEndDate().equals("CURRENT")) {
						log.info("Update existing appointment position-" + role.getSorId().trim());
						StringBuffer updateQuery = new StringBuffer();
						updateQuery.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						updateQuery.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n"); 
						updateQuery.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
						updateQuery.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n");
						updateQuery.append("DELETE { \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + dateTimeInterval + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#DateTimeValue> ?date . \n");
						//updateQuery.append("<" + qs.get("obj").toString() + "> ?p ?o . \n");
						updateQuery.append("} \n");
						updateQuery.append("INSERT { \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> core:DateTimeValue \"" + strDate + "\" . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> rdf:type core:DateTimeInterval . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> core:start <" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> vitro:mostSpecificType core:DateTimeInterval . \n");
						updateQuery.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						updateQuery.append("} \n");
						updateQuery.append("WHERE { \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + dateTimeInterval + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> <http://vivoweb.org/ontology/core#DateTimeValue> ?date . \n");
						//updateQuery.append("OPTIONAL {<" + qs.get("obj").toString() + "> ?p ?o .} \n");
						updateQuery.append("}");
						log.info(updateQuery.toString());
						response = this.vivoClient.vivoUpdateApi(updateQuery.toString());
						log.info(response);
						updateCount = updateCount + 1;
					}
					//Delete interim appointment
					else if(role.isInterimAppointment()) {
						StringBuffer updateQuery = new StringBuffer();
						updateQuery.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						updateQuery.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n"); 
						updateQuery.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
						updateQuery.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n");
						updateQuery.append("DELETE { \n");
						updateQuery.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> ?p ?o . \n");
						updateQuery.append("} \n");
						updateQuery.append("WHERE { \n");
						updateQuery.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> . \n");
						updateQuery.append("<" + this.vivoNamespace + "position-" + role.getSorId().trim() + "> ?p ?o . \n");
						updateQuery.append("}");
						log.info(updateQuery.toString());
						response = this.vivoClient.vivoUpdateApi(updateQuery.toString());
						log.info(response);
						
						updateCount = updateCount + 1;
					}
					else
						log.info("No updates are necessary for " + ob.getCwid().trim() + " for position-" + role.getSorId().trim());
				}
					
			} catch(Exception e) {
				log.error("Api Exception", e);
			}
			}
			//Checking for education and training updates
			for(EducationBean edu: ebean) {
				StringBuilder sb = new StringBuilder();
				
				sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
				sb.append("SELECT ?obj \n");
				sb.append("WHERE { \n");
				sb.append("GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> { \n");
				sb.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + ">");
				sb.append("}}");
				
				try {
					String response = this.vivoClient.vivoQueryApi(sb.toString());
					log.info(response);
					JSONObject obj = new JSONObject(response);
					JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
				
				if(bindings.isEmpty()) {
					log.info("Insert new education for " + ob.getCwid().trim() + " - educationalTraining-" + edu.getDegreePk().trim());
					edu.setCrudStatus("INSERT");
					StringBuffer insertQuery = new StringBuffer();
					insertQuery.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
					insertQuery.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
					insertQuery.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
					insertQuery.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
					insertQuery.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
					insertQuery.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
					insertQuery.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
					insertQuery.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> { \n");
					//for educational training
					insertQuery.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> rdf:type core:AwardedDegree . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> rdfs:label \"" + edu.getDegreeName().trim() + "\" . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:relates <" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:assignedBy <" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> .\n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0002353 <" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> .\n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:DateTimeValue \"" + strDate + "\" .\n");
					insertQuery.append("<" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> <http://vivoweb.org/ontology/core#abbreviation> \"" + edu.getDegreeName().trim() + "\" . \n");
					insertQuery.append("<" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> rdfs:label \"" + edu.getDegreeName().trim() + "\" . \n");
					insertQuery.append("<" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> core:relatedBy <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "degree/academicDegree" + edu.getBuiltInDegreePk().trim() + "> rdf:type <http://vivoweb.org/ontology/core#AcademicDegree> . \n");
					insertQuery.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> rdf:type <http://xmlns.com/foaf/0.1/Organization> .\n");
					insertQuery.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
					insertQuery.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> core:assigns <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> .\n");
					insertQuery.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
					
					//for educational process
					insertQuery.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> rdf:type core:EducationalProcess . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0000057 <" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0000057 <" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0002234 <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + edu.getDateTimeInterval().trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> core:DateTimeValue \"" + strDate + "\" .\n");
					insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + edu.getDateTimeInterval().trim() + "> rdf:type core:DateTimeInterval . \n");
					insertQuery.append("<" + this.vivoNamespace + "dtinterval-" + edu.getDateTimeInterval().trim() + "> core:end <" + this.vivoNamespace + "date-" + edu.getDateTimeInterval().substring(2).trim() + "> . \n");
					insertQuery.append("<" + this.vivoNamespace + "date-" + edu.getDateTimeInterval().substring(2).trim() + "> rdf:type core:DateTimeValue . \n");
					insertQuery.append("<" + this.vivoNamespace + "date-" + edu.getDateTimeInterval().substring(2).trim() + "> core:dateTimePrecision core:yearPrecision . \n");
					insertQuery.append("<" + this.vivoNamespace + "date-" + edu.getDateTimeInterval().substring(2).trim() + "> core:dateTime \"" + edu.getDateTimeInterval().substring(2).trim() + "\" . \n");
					insertQuery.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
					insertQuery.append("}}");
					
					log.info(insertQuery.toString());
					response = this.vivoClient.vivoUpdateApi(insertQuery.toString());
					log.info(response);
					updateCount = updateCount + 1;
					
				}
				else {
					//Check for change of institution
					
					edu.setCrudStatus("UPDATE");
					int instituteFk =0;
					sb.setLength(0);
					sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
					sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>");
					sb.append("SELECT ?instituteFk ?institutionLabel \n");
					sb.append("WHERE { \n");
					sb.append("GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> { \n");
					sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> core:assignedBy ?instituteFk . \n");
					sb.append("?instituteFk rdfs:label ?institutionLabel . \n");
					sb.append("}}");
					
					response = this.vivoClient.vivoQueryApi(sb.toString());
					log.info(response);
					obj = new JSONObject(response);
					bindings = obj.getJSONObject("results").getJSONArray("bindings");
					
					if(bindings.getJSONObject(0).optJSONObject("instituteFk") != null && bindings.getJSONObject(0).optJSONObject("instituteFk").has("value")) {
						instituteFk = Integer.parseInt(bindings.getJSONObject(0).optJSONObject("instituteFk").getString("value").replace(this.vivoNamespace + "org-", ""));
					}
					if(instituteFk != Integer.parseInt(edu.getInstituteFk())) {
						log.info("Insitition needs to be updated to " + edu.getInstituion() + " for educationalTraining-" + edu.getDegreePk().trim() + " with cwid " + ob.getCwid().trim());
						sb.setLength(0);
						sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>");
						sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
						sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
						sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n");
						sb.append("DELETE { \n");
						sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> core:assignedBy <" + this.vivoNamespace + "org-" + instituteFk + "> . \n");
						sb.append("<" + this.vivoNamespace + "org-" + instituteFk + "> core:assigns <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> .\n");
						sb.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0000057 <" + this.vivoNamespace + "org-" + instituteFk + "> . \n");
						sb.append("} \n");
						sb.append("INSERT { \n");
						sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> core:assignedBy <" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> core:assigns <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> .\n");
						sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> rdf:type <http://xmlns.com/foaf/0.1/Organization> .\n");
						sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> rdfs:label \"" + edu.getInstituion() + "\" .\n");
						sb.append("<" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" .\n");
						sb.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0000057 <" + this.vivoNamespace + "org-" + edu.getInstituteFk().trim() + "> . \n");
						sb.append("} \n");
						sb.append("WHERE { \n");
						sb.append("<" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk() + "> core:assignedBy <" + this.vivoNamespace + "org-" + instituteFk + "> . \n");
						sb.append("<" + this.vivoNamespace + "org-" + instituteFk + "> core:assigns <" + this.vivoNamespace + "educationalTraining-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> .\n");
						sb.append("<" + this.vivoNamespace + "educationalProcess-" + ob.getCwid().trim() + "-" + edu.getDegreePk().trim() + "> obo:RO_0000057 <" + this.vivoNamespace + "org-" + instituteFk + "> . \n");
						sb.append("}");
						
						log.info(sb.toString());
						response = this.vivoClient.vivoUpdateApi(sb.toString());
						log.info(response);
					} else if(instituteFk == Integer.parseInt(edu.getInstituteFk()) && bindings.getJSONObject(0).optJSONObject("institutionLabel") != null && bindings.getJSONObject(0).optJSONObject("institutionLabel").has("value") && !bindings.getJSONObject(0).optJSONObject("institutionLabel").getString("value").equals(edu.getInstituion())) {
						log.info("Insitition Label needs to be updated to " + edu.getInstituion() + " from " + bindings.getJSONObject(0).optJSONObject("institutionLabel").getString("value") + " for cwid " + ob.getCwid().trim());
						sb.setLength(0);
						sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>");
						sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n");
						sb.append("DELETE { \n");
						sb.append("<" + this.vivoNamespace + "org-" + instituteFk + "> rdfs:label ?institutionLabel .\n");
						sb.append("} \n");
						sb.append("INSERT { \n");
						sb.append("<" + this.vivoNamespace + "org-" + instituteFk + "> rdfs:label \"" + edu.getInstituion() + "\" .\n");
						sb.append("} \n");
						sb.append("WHERE { \n");
						sb.append("<" + this.vivoNamespace + "org-" + instituteFk + "> rdfs:label ?institutionLabel .\n");
						sb.append("}");
						
						log.info(sb.toString());
						response = this.vivoClient.vivoUpdateApi(sb.toString());
						log.info(response);
					} else	
						log.info("No updates are necessary for " + ob.getCwid().trim() + " for educationalTraining-" + edu.getDegreePk().trim());
				}
				} catch(Exception e) {
					log.error("Api Exception", e);
				}
			}
			
			
			insertInferenceTriples(ob);
			//Check if any appointment needs to be deleted in VIVO
			syncAppointmentsInVivoUsingTDB(rb, cwid);
			
			return updateCount;
			
		}
		
		
		/**
		 * This is the function which will sync appointments from ED to VIVO
		 * @param edRole This is the list of roles from Enterprise Directory
		 * @param cwid This is the unique identifier of the person
		 */
		private void syncAppointmentsInVivo(ArrayList<RoleBean> edRole, String cwid) {
			Date now = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			String strDate = sdf.format(now);
			ArrayList<RoleBean> vivoRole = new ArrayList<RoleBean>();
			
			StringBuilder sb = new StringBuilder();
			
			sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
			sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
			sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
			sb.append("SELECT ?position ?org ?start ?end ?dateTime \n");
			sb.append("WHERE {\n");
			sb.append("GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> {\n");
			sb.append("<" + this.vivoNamespace + "cwid-" + cwid + "> core:relatedBy ?position . \n");
			sb.append("?position a core:Position . \n");
			sb.append("?position core:relates ?org . \n");
			sb.append("?org rdf:type core:AcademicDepartment . \n");
			sb.append("?position core:dateTimeInterval ?dateTime . \n");
			sb.append("OPTIONAL {?dateTime a core:DateTimeInterval . }\n");
			sb.append("OPTIONAL {?dateTime core:start ?start . }\n");
			sb.append("OPTIONAL {?dateTime core:end ?end . }\n");
			sb.append("}}");

			
			TDBJenaConnect vivoJena = this.tcf.getConnectionfromPool("dataSet");
			
			try {
				ResultSet rs = runTDBSparqlTemplate(sb.toString(), vivoJena);
				
				
				while(rs.hasNext()) {
					QuerySolution qs = rs.nextSolution();
					RoleBean r = new RoleBean();
					if(qs.get("position")!= null) {
						r.setSorId(qs.get("position").toString().replace(this.vivoNamespace + "position-", "").trim());
					}
					
					if(qs.get("org")!= null) {
						r.setDeptCode(Integer.parseInt(qs.get("org").toString().replace(this.vivoNamespace + "org-u", "").trim()));
					}
					
					if(qs.get("start")!= null) {
						r.setStartDate(qs.get("start").toString().trim().replace(this.vivoNamespace + "date-", ""));
					}
					
					if(qs.get("end")!= null) {
						r.setEndDate(qs.get("end").toString().trim().replace(this.vivoNamespace + "date-", ""));
					}
					else
						r.setEndDate("CURRENT");
					
					vivoRole.add(r);
					
				}
			}
			catch(IOException e) {
				// TODO Auto-generated catch block
				log.error("IOException" ,e);
			}
			this.tcf.returnConnectionToPool(vivoJena, "dataSet");
			for(RoleBean r: vivoRole) {
				//try{
					//log.info(r.toString());
					if(edRole.stream().anyMatch(er -> er.getSorId().equals(r.getSorId()))) {
						log.info("The position - " + r.getSorId() + " exist in both ED and VIVO. Checking for date ranges.");
						//Check for date range
						if(edRole.stream().anyMatch(er -> er.getStartDate().equals(r.getStartDate()) && er.getEndDate().equals(r.getEndDate()))) {
							log.info("Date Range Matches. No change required.");
						}
						else {
							RoleBean role = edRole.stream().filter(er-> er.getSorId().equals(r.getSorId())).findFirst().get();
							log.info("Date does not match from ED : start - " + role.getStartDate() + " end - " + role.getEndDate() + " with VIVO : start - " + r.getStartDate() + " end - " + r.getEndDate());
							
							if(sb.length() > 0)
								sb.setLength(0);
							
							
							sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
							sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n"); 
							sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
							if(r.getStartDate() != null)
								sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n");
							if(r.getStartDate() != null) { //No Date Interval exist in the system
								sb.append("DELETE { \n");
								if(r.getEndDate().equals("CURRENT")) {
									sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to> . \n");
									//sb.append("<" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to> ?p ?o . \n");
								}
								else {
									sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to" + r.getEndDate().trim() + "> . \n");
									//sb.append("<" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to" + r.getEndDate().trim() + "> ?p ?o . \n");
								}
									
								sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> <http://vivoweb.org/ontology/core#DateTimeValue> ?date . \n");
								
								sb.append("} \n");
							}
							
							if(r.getStartDate() == null) {
								sb.append("INSERT DATA { \n");
								sb.append("GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> { \n");
							}
							else 
								sb.append("INSERT { \n");
							
							if(role.getEndDate().equals("CURRENT")) {
								sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> rdf:type core:DateTimeInterval . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> core:start <" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> vitro:mostSpecificType core:DateTimeInterval . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							else {
								sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> vitro:mostSpecificType core:DateTimeInterval . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> core:DateTimeValue \"" + strDate + "\" . \n");
							sb.append("} \n");
							if(r.getStartDate() == null) {
								sb.append("}");
							}
							if(r.getStartDate() != null) { //No Date Interval exist in the system
								sb.append("WHERE { \n");
								if(r.getEndDate().equals("CURRENT")) {
									sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to> . \n");
									//sb.append("<" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to> ?p ?o . \n");
								}
								else {
									sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to" + r.getEndDate().trim() + "> . \n");
									//sb.append("<" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to" + r.getEndDate().trim() + "> ?p ?o . \n");
								}
									
								sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> <http://vivoweb.org/ontology/core#DateTimeValue> ?date . \n");
								sb.append("}");
							}
							
							log.info(sb.toString());
							
							vivoJena = this.tcf.getConnectionfromPool("dataSet");
							
							try {
								runTDBSparqlUpdateTemplate(sb.toString(), vivoJena);
							} catch(IOException e) {
								log.error("IOException" ,e);
							}
							this.tcf.returnConnectionToPool(vivoJena, "dataSet");
						}
						
					}
					else {
						log.info("The position - " + r.getSorId() + " does not exist in ED anymore. Removing from VIVO");
						
						if(sb.length() > 0)
							sb.setLength(0);
						
						log.info("Deleting position - " + r.getSorId() + " from wcmcOfa graph");
						
						sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
						sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
						sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n");
						sb.append("DELETE { \n");
						sb.append("<" + this.vivoNamespace + "cwid-" + cwid + "> core:relatedBy <" + this.vivoNamespace + "position-" + r.getSorId() + "> . \n");
						sb.append("<" + this.vivoNamespace + "position-" + r.getSorId() + "> ?p ?o . \n");
						sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> core:relates <" + this.vivoNamespace + "org-u" + r.getDeptCode() + "> . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + r.getDeptCode() + "> core:relatedBy <" + this.vivoNamespace + "position-" + r.getSorId().trim() +"> . \n");
						sb.append("} \n");
						sb.append("WHERE { \n");
						sb.append("OPTIONAL { <" + this.vivoNamespace + "cwid-" + cwid + "> core:relatedBy <" + this.vivoNamespace + "position-" + r.getSorId() + "> . }\n");
						sb.append("OPTIONAL { <" + this.vivoNamespace + "position-" + r.getSorId() + "> ?p ?o . }\n");
						sb.append("OPTIONAL { <" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> core:relates <" + this.vivoNamespace + "org-u" + r.getDeptCode() + "> . }\n");
						sb.append("OPTIONAL { <" + this.vivoNamespace + "org-u" + r.getDeptCode() + "> core:relatedBy <" + this.vivoNamespace + "position-" + r.getSorId().trim() +"> . }\n");
						sb.append("}");
						
						log.info(sb.toString());
						
						vivoJena = this.tcf.getConnectionfromPool("dataSet");
						
						try {
							runTDBSparqlUpdateTemplate(sb.toString(), vivoJena);
						} catch(IOException e) {
							// TODO Auto-generated catch block
							log.error("IOException" ,e);
						}
						this.tcf.returnConnectionToPool(vivoJena, "dataSet");
						//Delete from inference Graph
						
						
						if(sb.length() > 0)
							sb.setLength(0);
						
						log.info("Deleting position - " + r.getSorId() + " from inference graph");
						
						sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
						sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
						sb.append("WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> \n");
						sb.append("DELETE { \n");
						sb.append("<" + this.vivoNamespace + "position-" + r.getSorId() + "> ?p ?o . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + r.getDeptCode() + "> core:contributingRole <" + this.vivoNamespace + "position-" + r.getSorId().trim() +"> . \n");
						sb.append("} \n");
						sb.append("WHERE { \n");
						sb.append("OPTIONAL { <" + this.vivoNamespace + "position-" + r.getSorId() + "> ?p ?o . }\n");
						sb.append("OPTIONAL { <" + this.vivoNamespace + "org-u" + r.getDeptCode() + "> core:contributingRole <" + this.vivoNamespace + "position-" + r.getSorId().trim() +"> . }\n");
						sb.append("}");
						
						log.info(sb.toString());
						
						vivoJena = this.tcf.getConnectionfromPool("dataSet");
						
						try {
							runTDBSparqlUpdateTemplate(sb.toString(), vivoJena);
						} catch(IOException e) {
							// TODO Auto-generated catch block
							log.error("IOException" ,e);
						}
						this.tcf.returnConnectionToPool(vivoJena, "dataSet");
						
					}
				/*}
				catch(NoSuchElementException nse) {
					log.info("The position does not exist in VIVO");
				}*/
			}
		}

		/**
		 * This is the function which will sync appointments from ED to VIVO
		 * @param edRole This is the list of roles from Enterprise Directory
		 * @param cwid This is the unique identifier of the person
		 */
		private void syncAppointmentsInVivoUsingTDB(ArrayList<RoleBean> edRole, String cwid) {
			Date now = new Date();
			String strDate = sdf.format(now);
			ArrayList<RoleBean> vivoRole = new ArrayList<RoleBean>();
			
			StringBuilder sb = new StringBuilder();
			
			sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
			sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
			sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
			sb.append("SELECT ?position ?org ?start ?end ?dateTime \n");
			sb.append("WHERE {\n");
			sb.append("GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> { \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + cwid + "> core:relatedBy ?position . \n");
			sb.append("?position a core:Position . \n");
			sb.append("?position core:relates ?org . \n");
			sb.append("?org rdf:type core:AcademicDepartment . \n");
			sb.append("?position core:dateTimeInterval ?dateTime . \n");
			sb.append("OPTIONAL {?dateTime a core:DateTimeInterval . }\n");
			sb.append("OPTIONAL {?dateTime core:start ?start . }\n");
			sb.append("OPTIONAL {?dateTime core:end ?end . }\n");
			sb.append("}}");

			try {
				String response = vivoClient.vivoQueryApi(sb.toString());
				log.info(response);
				JSONObject obj = new JSONObject(response);
				JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
				if(bindings != null && !bindings.isEmpty()) {
					for (int i = 0; i < bindings.length(); ++i) {
						RoleBean r = new RoleBean();
						if(bindings.getJSONObject(i).optJSONObject("position") != null && bindings.getJSONObject(i).optJSONObject("position").has("value")) {
							r.setSorId(bindings.getJSONObject(i).getJSONObject("position").getString("value").replace(this.vivoNamespace + "position-", "").trim());
						}
						
						if(bindings.getJSONObject(i).optJSONObject("org") != null && bindings.getJSONObject(i).optJSONObject("org").has("value")) {
							r.setDeptCode(Integer.parseInt(bindings.getJSONObject(i).getJSONObject("org").getString("value").replace(this.vivoNamespace + "org-u", "").trim()));
						}
						
						if(bindings.getJSONObject(i).optJSONObject("start") != null && bindings.getJSONObject(i).optJSONObject("start").has("value")) {
							r.setStartDate(bindings.getJSONObject(i).getJSONObject("start").getString("value").replace(this.vivoNamespace + "date-", ""));
						}
						
						if(bindings.getJSONObject(i).optJSONObject("end") != null && bindings.getJSONObject(i).optJSONObject("end").has("value")) {
							r.setEndDate(bindings.getJSONObject(i).getJSONObject("end").getString("value").replace(this.vivoNamespace + "date-", ""));
						}
						else
							r.setEndDate("CURRENT");
						
						vivoRole.add(r);
						
					}
				}
			}
			catch(Exception e) {
				log.error("API Exception" ,e);
			}
			
			for(RoleBean r: vivoRole) {
				//try{
					//log.info(r.toString());
					if(edRole.stream().anyMatch(er -> er.getSorId().equals(r.getSorId()))) {
						log.info("The position - " + r.getSorId() + " exist in both ED and VIVO. Checking for date ranges.");
						//Check for date range
						if(edRole.stream().anyMatch(er -> er.getStartDate().equals(r.getStartDate()) && er.getEndDate().equals(r.getEndDate()))) {
							log.info("Date Range Matches. No change required.");
						}
						else {
							RoleBean role = edRole.stream().filter(er-> er.getSorId().equals(r.getSorId())).findFirst().get();
							log.info("Date does not match from ED : start - " + role.getStartDate() + " end - " + role.getEndDate() + " with VIVO : start - " + r.getStartDate() + " end - " + r.getEndDate());
							
							if(sb.length() > 0)
								sb.setLength(0);
							
							
							sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
							sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n"); 
							sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
							if(r.getStartDate() != null)
								sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n");
							if(r.getStartDate() != null) { //No Date Interval exist in the system
								sb.append("DELETE { \n");
								if(r.getEndDate().equals("CURRENT")) {
									sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to> . \n");
									//sb.append("<" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to> ?p ?o . \n");
								}
								else {
									sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to" + r.getEndDate().trim() + "> . \n");
									//sb.append("<" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to" + r.getEndDate().trim() + "> ?p ?o . \n");
								}
									
								sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> <http://vivoweb.org/ontology/core#DateTimeValue> ?date . \n");
								
								sb.append("} \n");
							}
							
							if(r.getStartDate() == null) {
								sb.append("INSERT DATA { \n");
								sb.append("GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> { \n");
							}
							else 
								sb.append("INSERT { \n");
							
							if(role.getEndDate().equals("CURRENT")) {
								sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> rdf:type core:DateTimeInterval . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> core:start <" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> vitro:mostSpecificType core:DateTimeInterval . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							else {
								sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-" + role.getStartDate().trim() + "> . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-" + role.getEndDate().trim() + "> . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> vitro:mostSpecificType core:DateTimeInterval . \n");
								sb.append("<" + this.vivoNamespace + "dtinterval-" + role.getStartDate().trim() + "to" + role.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> core:DateTimeValue \"" + strDate + "\" . \n");
							sb.append("} \n");
							if(r.getStartDate() == null) {
								sb.append("}");
							}
							if(r.getStartDate() != null) { //No Date Interval exist in the system
								sb.append("WHERE { \n");
								if(r.getEndDate().equals("CURRENT")) {
									sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to> . \n");
									//sb.append("<" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to> ?p ?o . \n");
								}
								else {
									sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> <http://vivoweb.org/ontology/core#dateTimeInterval> <" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to" + r.getEndDate().trim() + "> . \n");
									//sb.append("<" + this.vivoNamespace + "dtinterval-" + r.getStartDate().trim() + "to" + r.getEndDate().trim() + "> ?p ?o . \n");
								}
									
								sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> <http://vivoweb.org/ontology/core#DateTimeValue> ?date . \n");
								sb.append("}");
							}
							
							log.info(sb.toString());
							try{
								String response = this.vivoClient.vivoUpdateApi(sb.toString());
								log.info(response);
							} catch(Exception  e) {
								log.info("Api Exception", e);
							}
						}
						
					}
					else {
						log.info("The position - " + r.getSorId() + " does not exist in ED anymore. Removing from VIVO");
						
						if(sb.length() > 0)
							sb.setLength(0);
						
						log.info("Deleting position - " + r.getSorId() + " from wcmcOfa graph");
						
						sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
						sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
						sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n");
						sb.append("DELETE { \n");
						sb.append("<" + this.vivoNamespace + "cwid-" + cwid + "> core:relatedBy <" + this.vivoNamespace + "position-" + r.getSorId() + "> . \n");
						sb.append("<" + this.vivoNamespace + "position-" + r.getSorId() + "> ?p ?o . \n");
						sb.append("<" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> core:relates <" + this.vivoNamespace + "org-u" + r.getDeptCode() + "> . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + r.getDeptCode() + "> core:relatedBy <" + this.vivoNamespace + "position-" + r.getSorId().trim() +"> . \n");
						sb.append("} \n");
						sb.append("WHERE { \n");
						sb.append("OPTIONAL { <" + this.vivoNamespace + "cwid-" + cwid + "> core:relatedBy <" + this.vivoNamespace + "position-" + r.getSorId() + "> . }\n");
						sb.append("OPTIONAL { <" + this.vivoNamespace + "position-" + r.getSorId() + "> ?p ?o . }\n");
						sb.append("OPTIONAL { <" + this.vivoNamespace + "position-" + r.getSorId().trim() + "> core:relates <" + this.vivoNamespace + "org-u" + r.getDeptCode() + "> . }\n");
						sb.append("OPTIONAL { <" + this.vivoNamespace + "org-u" + r.getDeptCode() + "> core:relatedBy <" + this.vivoNamespace + "position-" + r.getSorId().trim() +"> . }\n");
						sb.append("}");
						
						log.info(sb.toString());
						
						try{
							String response = this.vivoClient.vivoUpdateApi(sb.toString());
							log.info(response);
						} catch(Exception  e) {
							log.info("Api Exception", e);
						}
						
						//Delete from inference Graph
						
						
						if(sb.length() > 0)
							sb.setLength(0);
						
						log.info("Deleting position - " + r.getSorId() + " from inference graph");
						
						sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
						sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
						sb.append("WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> \n");
						sb.append("DELETE { \n");
						sb.append("<" + this.vivoNamespace + "position-" + r.getSorId() + "> ?p ?o . \n");
						sb.append("<" + this.vivoNamespace + "org-u" + r.getDeptCode() + "> core:contributingRole <" + this.vivoNamespace + "position-" + r.getSorId().trim() +"> . \n");
						sb.append("} \n");
						sb.append("WHERE { \n");
						sb.append("OPTIONAL { <" + this.vivoNamespace + "position-" + r.getSorId() + "> ?p ?o . }\n");
						sb.append("OPTIONAL { <" + this.vivoNamespace + "org-u" + r.getDeptCode() + "> core:contributingRole <" + this.vivoNamespace + "position-" + r.getSorId().trim() +"> . }\n");
						sb.append("}");
						
						log.info(sb.toString());
						
						try{
							String response = this.vivoClient.vivoUpdateApi(sb.toString());
							log.info(response);
						} catch(Exception  e) {
							log.info("Api Exception", e);
						} 
						
						
					}
				/*}
				catch(NoSuchElementException nse) {
					log.info("The position does not exist in VIVO");
				}*/
			}
		}
			
		
		/**
		 * This function returns true or false based on the OFA data for that faculty exists in VIVO
		 * @param ob The bean object containing role and education & training data both
		 * @return true or false based on the OFA data for that faculty exists in VIVO
		 * @throws IOException thrown by SDBJenaConnect
		 */
		private boolean checkOfaDataInVivo(OfaBean ob) throws IOException {
			int count = 0;
			StringBuffer sb = new StringBuffer();
			sb.append("SELECT  (count(?o) as ?positionCount) \n");
			sb.append("WHERE \n");
			sb.append("{ \n");
			sb.append("GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> { \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + ob.getCwid().trim() + "> ?p ?o . \n");
			sb.append("}}");
			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				try{
					String response = this.vivoClient.vivoQueryApi(sb.toString());
					log.info(response);
					JSONObject obj = new JSONObject(response);
					JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
					count = bindings.getJSONObject(0).getJSONObject("positionCount").getInt("value");
				} catch(Exception  e) {
					log.info("Api Exception", e);
				}
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
				ResultSet rs = runSparqlTemplate(sb.toString(), vivoJena);
				QuerySolution qs = rs.nextSolution();
				count = Integer.parseInt(qs.get("positionCount").toString().replace("^^http://www.w3.org/2001/XMLSchema#integer", ""));
				//Close the connection
				if(vivoJena!= null)
					this.jcf.returnConnectionToPool(vivoJena, "dataSet");
			} else {
				TDBJenaConnect vivoJena = this.tcf.getConnectionfromPool("dataSet");
				ResultSet rs = vivoJena.executeSelectQuery(sb.toString(), true);
				QuerySolution qs = rs.nextSolution();
				count = Integer.parseInt(qs.get("positionCount").toString().replace("^^http://www.w3.org/2001/XMLSchema#integer", ""));
				//Close the connection
				if(vivoJena!= null)
					this.tcf.returnConnectionToPool(vivoJena, "dataSet");
			}
			
			if(count > 0)
				return true;
			
			return false;	
			
		}
		
		/**
		 * Template to fit in different JenaConnect queries.
		 * @param sparqlQuery contains the query
		 * @return ResultSet containing all the results
		 * @throws IOException default exception thrown
		 */
		private ResultSet runSparqlTemplate(String sparqlQuery, SDBJenaConnect vivoJena) throws IOException {		
			return vivoJena.executeSelectQuery(sparqlQuery, true);
		}
		
		/**
		 * Template to fit in different JenaConnect queries.
		 * @param sparqlQuery contains the query
		 * @param vivoJena connection to SDB jenas
		 * @throws IOException default exception thrown
		 */
		private void runSparqlUpdateTemplate(String sparqlQuery, SDBJenaConnect vivoJena) throws IOException {
			vivoJena.executeUpdateQuery(sparqlQuery, true);
			//log.info("Inserted success");
		
		}

		/**
		 * Template to fit in different JenaConnect queries.
		 * @param sparqlQuery contains the query
		 * @return ResultSet containing all the results
		 * @throws IOException default exception thrown
		 */
		private ResultSet runTDBSparqlTemplate(String sparqlQuery, TDBJenaConnect vivoJena) throws IOException {		
			return vivoJena.executeSelectQuery(sparqlQuery, true);
		}
		
		/**
		 * Template to fit in different JenaConnect queries.
		 * @param sparqlQuery contains the query
		 * @param vivoJena connection to SDB jenas
		 * @throws IOException default exception thrown
		 */
		private void runTDBSparqlUpdateTemplate(String sparqlQuery, TDBJenaConnect vivoJena) throws IOException {
			vivoJena.executeUpdateQuery(sparqlQuery, true);
		}
		/**
	     * This function generates a hashcode for a department name(Not used in this class)
	     * @param dept department name
	     * @return hashcode for the department
	     */
		public int keyHash(String dept) {
			int hash = 7;
			for (int i = 0; i < dept.length(); i++) {
				hash = hash*31 + dept.charAt(i);
			}
			hash = hash & Integer.MAX_VALUE;
			return hash;
		}
	
}
