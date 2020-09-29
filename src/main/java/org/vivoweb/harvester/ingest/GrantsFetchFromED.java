package org.vivoweb.harvester.ingest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringEscapeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;

import lombok.extern.slf4j.Slf4j;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.json.JSONArray;
import org.json.JSONObject;

import reciter.connect.beans.vivo.GrantBean;
import reciter.connect.database.mssql.MssqlConnectionFactory;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;
import reciter.connect.vivo.api.client.VivoClient;

/**
 * @author Sarbajit Dutta (szd2013@med.cornell.edu)
 * <p><b><i>This class fetches grant data from Coeus for faculty who are in VIVO and inserts or updates them in VIVO. 
 * Since the people data comes from ED the data that is used by this class is for faculty who are active in ED.
 * Also, the organization structure uses the old org-hierarchy scripts used in D2RMAP. For updates the data checks for new contributors and date end(for now).<p><b><i>
 */
@Slf4j
@Component
public class GrantsFetchFromED {
	
	public static String propertyFilePath = null;

	
	/**
	 * Variables for counts
	 */
	private int insertCount = 0;
	private int updateCount = 0;
	
	/**
	 * MySql connection factory object for all the mysql related connections
	 */
	@Autowired
	private MssqlConnectionFactory mcf;
	
	/**
	 * Jena connection factory object for all the apache jena sdb related connections
	 */
	@Autowired
	private JenaConnectionFactory jcf;

	@Autowired
	private VivoClient vivoClient;
	
	/**
	 * The default namespace for VIVO
	 */
	private String vivoNamespace = JenaConnectionFactory.nameSpace;
	
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	Date now = new Date();
	/**
	 * This sets todays date for harvested date
	 */
	private String strDate = this.sdf.format(this.now);

	public Callable<String> getCallable(List<String> people) {
        return new Callable<String>() {
            public String call() throws Exception {
                return execute(people);
            }
        };
    }
		
		/**
		 * This is the main execution method of the class
		 */
		public String execute(List<String> people) {
			List<GrantBean> grant = null;
			Iterator<String> it = people.iterator();
			while(it.hasNext()) {
				String cwid = it.next().trim();
				log.info("#########################################################");
				log.info("Trying to fetch grants for cwid - " + cwid);
				grant = getGrantsFromCoeus(cwid, people);
				if(grant.isEmpty())
					log.info("There is no grants for cwid - " + cwid + " in Coeus");
				checkGrantExistInVivo(grant,cwid);
				//deleteConfidentialGrants(grant, cwid);
				log.info("#########################################################");
			}
			
		/*log.info("#########################################################");
		log.info("Trying to fetch grants for cwid - arj2005");
		grant = getGrantsFromCoeus("arj2005");
		if(grant.isEmpty())
			log.info("There is no grants for cwid - arj2005 in Coeus");
		checkGrantExistInVivo(grant,"arj2005");
		deleteConfidentialGrants(grant, "arj2005");
		log.info("#########################################################");*/

			log.info("Total new grants inserted into VIVO: " + this.insertCount);
			log.info("Total existing grants that were updated: " + this.updateCount);
			
			return "Grants fetch completed successfully for cwids: " + people.toString();
		}
		
		/**
		 * This method check for grants coming from Coeus whether they exist in VIVO
		 * @param grants the list of grants
		 * @param cwid unique identifier for faculty
		 */
		private void checkGrantExistInVivo(List<GrantBean> grants, String cwid) {
			for(int i=0; i< grants.size(); i++) {
				String sparqlQuery = "PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
					 "PREFIX foaf:     <http://xmlns.com/foaf/0.1/> \n" +
					 "SELECT  (count(?o) as ?grant) \n" +
					 "FROM <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> \n" +
					 "WHERE \n" +
					 "{ \n" +
					 "<" + this.vivoNamespace + "grant-" + grants.get(i).getAwardNumber().trim() + "> ?p ?o . \n" +
					 "}";
				try {
					String response = this.vivoClient.vivoQueryApi(sparqlQuery);
					log.info(response);
					JSONObject obj = new JSONObject(response);
					JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
					int count = bindings.getJSONObject(0).getJSONObject("grant").getInt("value");
					if(count > 0) {
						log.info("Grant- " + grants.get(i).getAwardNumber() + " exists in VIVO");
						//This is done to return the connection for coeus since it is being used again in the update function
						checkForUpdates(grants.get(i), cwid, "UPDATE");
					}
					else {
						insertGrantsInVivo(grants.get(i),cwid,"INSERT");
						this.insertCount = this.insertCount + 1;
					}
				} catch(Exception e) {
					log.error("Api Exception", e);
				}
			}
		}
		
		
		/**
		 * Check for updates function will check for updates in coeus against Vivo for grant end dates and 
		 * contributors(for now we might want to expand that later)
		 * @param gb is the grant bean with all the grant data
		 */
		private void checkForUpdates(GrantBean gb, String cwid, String crudStatus) {
			
			List<String> contributors = new ArrayList<String>();
			String dateTimeInterval = null;
			boolean dateTimeIntervalFlag = false;
			String beginDate = null;
			String endDate = null;
			DateFormat shortFormat = new SimpleDateFormat("yyyy-MM-dd",Locale.ENGLISH);
			DateFormat mediumFormat = new SimpleDateFormat("dd-MMM-yy",Locale.ENGLISH);
			DateFormat yearFormat = new SimpleDateFormat("yyyy",Locale.ENGLISH);
			//get contributor list & date interval for that grant from VIVO
			StringBuilder sb = new StringBuilder();
			sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
			sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
			sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n");
			sb.append("select ?person ?dateTimeInterval \n");
			sb.append("from <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> \n");
			sb.append("where { \n");
			sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates ?person . \n");
			sb.append("?person rdf:type foaf:Person . \n");
			sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:dateTimeInterval ?dateTimeInterval . \n");
			sb.append("}");
			
			//log.info(sb.toString());
			
			try{
				String response = vivoClient.vivoQueryApi(sb.toString());
				log.info(response);
				JSONObject obj = new JSONObject(response);
				JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
				if(bindings != null && !bindings.isEmpty()) {
					for (int i = 0; i < bindings.length(); ++i) {
						contributors.add(bindings.getJSONObject(i).getJSONObject("person").getString("value").replace(this.vivoNamespace + "cwid-", ""));
					}
					dateTimeInterval = bindings.getJSONObject(0).getJSONObject("dateTimeInterval").getString("value").replace(this.vivoNamespace + "dtinterval-", "").trim();
				} else {
					log.info("No result from the query");
				}
			} catch(Exception e) {
				log.error("API Exception" ,e);
			}
			
			//Checking for new contributors
			Map<String, String> newContributors = new HashMap<String, String>();
			Iterator<Entry<String, String>> it = gb.getContributors().entrySet().iterator();
			while(it.hasNext()) {
				Entry<String, String> pair = it.next();
				String contrib = pair.getKey();
				String ctype = pair.getValue();
				if(!contributors.contains(contrib))
					newContributors.put(contrib,ctype);
					
			}
			
			if(newContributors.isEmpty())
				log.info("No new contributors for grant-" + gb.getAwardNumber().trim());
			
			//Checking for new Date Time Interval
			
			String[] dates = dateTimeInterval.split("to");
			if(dates[1].trim().equals(gb.getEndDate().trim()) && dateTimeInterval != null) 
				log.info("Grant Date Interval has not changed");
			else
				dateTimeIntervalFlag = true;
			
			beginDate = dates[0].trim();
			endDate = dates[1].trim();
			try {
				if(!beginDate.equals(""))
					beginDate = shortFormat.format(mediumFormat.parse(beginDate));
				if(!endDate.equals(""))
					endDate = shortFormat.format(mediumFormat.parse(endDate));
			} catch(ParseException e) {
				log.error("ParseException", e);
			}
			
			
			sb.setLength(0);
			//Make the updates to VIVO
			if(!newContributors.isEmpty() || dateTimeIntervalFlag) {
				log.info("Making updates for grant-" + gb.getAwardNumber());
				sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
				sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
				sb.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
				sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
				sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
				sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
				sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
				if(dateTimeIntervalFlag) {
					sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> \n");
					sb.append("DELETE { \n");
					sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + dates[0].trim() + "to" + dates[1].trim() + "> . \n");
					//Start Date Section
					if(!beginDate.equals("")) {
						sb.append("<" + this.vivoNamespace + "date-" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
						sb.append("<" + this.vivoNamespace + "date-" + beginDate.trim() + "> core:dateTimePrecision core:yearMonthDayPrecision . \n");
						sb.append("<" + this.vivoNamespace + "date-" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "T00:00:00\" . \n" );
						sb.append("<" + this.vivoNamespace + "date-" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
					}
					//End Date Section
					if(!endDate.equals("")) {
						sb.append("<" + this.vivoNamespace + "date-" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
						sb.append("<" + this.vivoNamespace + "date-" + endDate.trim() + "> core:dateTimePrecision core:yearMonthDayPrecision . \n");
						sb.append("<" + this.vivoNamespace + "date-" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "T00:00:00\" . \n" );
						sb.append("<" + this.vivoNamespace + "date-" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						sb.append("<" + this.vivoNamespace + "dtinterval-" + dates[0].trim() + "to" + dates[1].trim() + "> ?p ?o . \n");
					}
					sb.append("} \n");
				}
				if(!dateTimeIntervalFlag && !newContributors.isEmpty()){
					sb.append("INSERT DATA { \n");
				}
				else
					sb.append("INSERT { \n");
				if(!newContributors.isEmpty()) {
					if(!dateTimeIntervalFlag) {
						sb.append("GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> { \n");
					}
					it = newContributors.entrySet().iterator();
					while(it.hasNext()) {
						Entry<String, String> pair = it.next();
						String contributor = pair.getKey();
						String ctype = pair.getValue();
						
						if(ctype.equals("PrincipalInvestigatorRole")) {
							if(cwid.equals(contributor)) 
								sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> obo:RO_0000053 <" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type core:PrincipalInvestigatorRole . \n");
							sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> obo:RO_0000052 <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> vitro:mostSpecificType core:PrincipalInvestigatorRole . \n");
							sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:relatedBy <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> .\n");
							sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							beginDate = gb.getBeginDate().trim();
							endDate = gb.getEndDate().trim();
							try {
								if(!beginDate.equals(""))
									beginDate = yearFormat.format(mediumFormat.parse(beginDate));
								if(!endDate.equals(""))
									endDate = yearFormat.format(mediumFormat.parse(endDate));
							} catch(ParseException e) {
								log.error("ParseException", e);
							}
							//Date Time Interval
							sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-role-pi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-pi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
							if(!gb.getBeginDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-pi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-role" + beginDate.trim() + "> . \n");
							
							if(!gb.getEndDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-pi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-role" + endDate.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-pi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							
							//Start Date Section
							if(!gb.getBeginDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							//End Date Section
							if(!gb.getEndDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
						}
						else if(ctype.equals("KeyPersonnelRole")) {
							if(cwid.equals(contributor))
								sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> obo:RO_0000053 <" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type wcmc:KeyPersonnelRole . \n");
							sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> obo:RO_0000052 <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> vitro:mostSpecificType wcmc:KeyPersonnelRole . \n");
							sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:relatedBy <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> .\n");
							sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							beginDate = gb.getBeginDate().trim();
							endDate = gb.getEndDate().trim();
							try {
								if(!beginDate.equals(""))
									beginDate = yearFormat.format(mediumFormat.parse(beginDate));
								if(!endDate.equals(""))
									endDate = yearFormat.format(mediumFormat.parse(endDate));
							} catch(ParseException e) {
								log.error("ParseException", e);
							}
							//Date Time Interval
							sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-role-kp-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-kp-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
							if(!gb.getBeginDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-kp-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-role" + beginDate.trim() + "> . \n");
							
							if(!gb.getEndDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-kp-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-role" + endDate.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-kp-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							
							//Start Date Section
							if(!gb.getBeginDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							//End Date Section
							if(!gb.getEndDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
						}
						else if(ctype.equals("PrincipalInvestigatorSubawardRole")) {
							if(cwid.equals(contributor))
								sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> obo:RO_0000053 <" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type wcmc:PrincipalInvestigatorSubawardRole . \n");
							sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> obo:RO_0000052 <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> vitro:mostSpecificType wcmc:PrincipalInvestigatorSubawardRole . \n");
							sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:relatedBy <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> .\n");
							sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							beginDate = gb.getBeginDate().trim();
							endDate = gb.getEndDate().trim();
							try {
								if(!beginDate.equals(""))
									beginDate = yearFormat.format(mediumFormat.parse(beginDate));
								if(!endDate.equals(""))
									endDate = yearFormat.format(mediumFormat.parse(endDate));
							} catch(ParseException e) {
								log.error("ParseException", e);
							}
							//Date Time Interval
							sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-role-pisa-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-pisa-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
							if(!gb.getBeginDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-pisa-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-role" + beginDate.trim() + "> . \n");
							
							if(!gb.getEndDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-pisa-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-role" + endDate.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-pisa-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							
							//Start Date Section
							if(!gb.getBeginDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							//End Date Section
							if(!gb.getEndDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
						}
						else if(ctype.equals("CoPrincipalInvestigatorRole")) {
							if(cwid.equals(contributor))
								sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> obo:RO_0000053 <" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type vivo:CoPrincipalInvestigatorRole . \n");
							sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> obo:RO_0000052 <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> vitro:mostSpecificType vivo:CoPrincipalInvestigatorRole . \n");
							sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:relatedBy <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> .\n");
							sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							beginDate = gb.getBeginDate().trim();
							endDate = gb.getEndDate().trim();
							try {
								if(!beginDate.equals(""))
									beginDate = yearFormat.format(mediumFormat.parse(beginDate));
								if(!endDate.equals(""))
									endDate = yearFormat.format(mediumFormat.parse(endDate));
							} catch(ParseException e) {
								log.error("ParseException", e);
							}
							//Date Time Interval
							sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-role-copi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-copi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
							if(!gb.getBeginDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-copi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-role" + beginDate.trim() + "> . \n");
							
							if(!gb.getEndDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-copi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-role" + endDate.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-copi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							
							//Start Date Section
							if(!gb.getBeginDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							//End Date Section
							if(!gb.getEndDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
						}
						else if(ctype.equals("CoInvestigatorRole")) {
							if(cwid.equals(contributor))
								sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> obo:RO_0000053 <" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type wcmc:CoInvestigatorRole . \n");
							sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> obo:RO_0000052 <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> vitro:mostSpecificType wcmc:CoInvestigatorRole . \n");
							sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:relatedBy <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> .\n");
							sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							beginDate = gb.getBeginDate().trim();
							endDate = gb.getEndDate().trim();
							try {
								if(!beginDate.equals(""))
									beginDate = yearFormat.format(mediumFormat.parse(beginDate));
								if(!endDate.equals(""))
									endDate = yearFormat.format(mediumFormat.parse(endDate));
							} catch(ParseException e) {
								log.error("ParseException", e);
							}
							//Date Time Interval
							sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-role-coi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-coi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
							if(!gb.getBeginDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-coi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-role" + beginDate.trim() + "> . \n");
							
							if(!gb.getEndDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-coi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-role" + endDate.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-coi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							
							//Start Date Section
							if(!gb.getBeginDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							//End Date Section
							if(!gb.getEndDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
						}
					}
					if(!dateTimeIntervalFlag && !newContributors.isEmpty())
						sb.append("} \n");
					
				}
				if(dateTimeIntervalFlag) {
					
					String insertBeginDate = gb.getBeginDate().trim();
					String insertEndDate = gb.getEndDate().trim();
					try {
						if(!insertBeginDate.equals(""))
							insertBeginDate = shortFormat.format(mediumFormat.parse(insertBeginDate));
						if(!insertEndDate.equals(""))
							insertEndDate = shortFormat.format(mediumFormat.parse(insertEndDate));
					} catch(ParseException e) {
						log.error("ParseException", e);
					}
					
					sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
					sb.append("<" + this.vivoNamespace + "dtinterval-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
					if(!gb.getBeginDate().equals(""))
						sb.append("<" + this.vivoNamespace + "dtinterval-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-" + insertBeginDate.trim() + "> . \n");
					if(!gb.getEndDate().equals(""))
						sb.append("<" + this.vivoNamespace + "dtinterval-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-" + insertEndDate.trim() + "> . \n");
					sb.append("<" + this.vivoNamespace + "dtinterval-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
					//Start Date Section
					if(!gb.getBeginDate().equals("")) {
						sb.append("<" + this.vivoNamespace + "date-" + insertBeginDate.trim() + "> rdf:type core:DateTimeValue . \n");
						sb.append("<" + this.vivoNamespace + "date-" + insertBeginDate.trim() + "> core:dateTimePrecision core:yearMonthDayPrecision . \n");
						sb.append("<" + this.vivoNamespace + "date-" + insertBeginDate.trim() + "> core:dateTime \"" + insertBeginDate.trim() + "T00:00:00\" . \n" );
						sb.append("<" + this.vivoNamespace + "date-" + insertBeginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
					}
					//End Date Section
					if(!gb.getEndDate().equals("")) {
						sb.append("<" + this.vivoNamespace + "date-" + insertEndDate.trim() + "> rdf:type core:DateTimeValue . \n");
						sb.append("<" + this.vivoNamespace + "date-" + insertEndDate.trim() + "> core:dateTimePrecision core:yearMonthDayPrecision . \n");
						sb.append("<" + this.vivoNamespace + "date-" + insertEndDate.trim() + "> core:dateTime \"" + insertEndDate.trim() + "T00:00:00\" . \n" );
						sb.append("<" + this.vivoNamespace + "date-" + insertEndDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
					}
				}
				sb.append("} \n");
				if(dateTimeIntervalFlag) {
					sb.append("WHERE { \n");
					sb.append("OPTIONAL {<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + dates[0].trim() + "to" + dates[1].trim() + "> . \n");
					
					//Start Date Section
					sb.append("<" + this.vivoNamespace + "date-" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
					sb.append("<" + this.vivoNamespace + "date-" + beginDate.trim() + "> core:dateTimePrecision core:yearMonthDayPrecision . \n");
					sb.append("<" + this.vivoNamespace + "date-" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "T00:00:00\" . \n" );
					sb.append("<" + this.vivoNamespace + "date-" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
					//End Date Section
					sb.append("<" + this.vivoNamespace + "date-" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
					sb.append("<" + this.vivoNamespace + "date-" + endDate.trim() + "> core:dateTimePrecision core:yearMonthDayPrecision . \n");
					sb.append("<" + this.vivoNamespace + "date-" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "T00:00:00\" . \n" );
					sb.append("<" + this.vivoNamespace + "date-" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
					sb.append("<" + this.vivoNamespace + "dtinterval-" + dates[0].trim() + "to" + dates[1].trim() + "> ?p ?o . \n");
					sb.append("}}");
				}
				
				//log.info(sb.toString());
				try {
					String response = this.vivoClient.vivoUpdateApi(sb.toString());
					log.info(response);
				} catch(Exception e) {
					log.error("Error in updating grant data: ", e);
				}
				
				
				
				gb.setContributors(newContributors);
				
				/* if(!newContributors.isEmpty())
					insertInferenceTriples(gb, crudStatus); */
				
				this.updateCount = this.updateCount + 1;
			}
			else
				log.info("No updates are necessary for grant-" + gb.getAwardNumber());
			checkForSponsorUpdate(gb);
		}
		
		/**
		 * This function is to sync Sponsor Code and Label
		 * @param gb
		 */
		private void checkForSponsorUpdate(GrantBean gb) {			
			//get contributor list & date interval for that grant from VIVO
			StringBuilder sb = new StringBuilder();
			sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
			sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
			sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
			sb.append("select ?fundingOrganization ?fundingOrganizationLabel \n");
			sb.append("from <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> \n");
			sb.append("where { \n");
			sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:assignedBy ?fundingOrganization . \n");
			sb.append("?fundingOrganization rdf:type core:FundingOrganization . \n");
			sb.append("?fundingOrganization rdfs:label ?fundingOrganizationLabel . \n");
			sb.append("}");
			
			//log.info(sb.toString());
			
			try{
				String response = this.vivoClient.vivoQueryApi(sb.toString());
				log.info(response);
				JSONObject obj = new JSONObject(response);
				JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
				if(bindings != null && !bindings.isEmpty()) {
					if(bindings.getJSONObject(0).optJSONObject("fundingOrganization") != null && bindings.getJSONObject(0).optJSONObject("fundingOrganization").has("value") &&
					bindings.getJSONObject(0).optJSONObject("fundingOrganizationLabel") != null && bindings.getJSONObject(0).optJSONObject("fundingOrganizationLabel").has("value")) {
						if(bindings.getJSONObject(0).getJSONObject("fundingOrganization").getString("value").contains(this.vivoNamespace + "org-f")) {
							String fundingOrganizationCode = bindings.getJSONObject(0).getJSONObject("fundingOrganization").getString("value").replace(this.vivoNamespace + "org-f", "").trim();
							String fundingOrganizationLabel = bindings.getJSONObject(0).getJSONObject("fundingOrganizationLabel").getString("value");
							//Update only label
							if(fundingOrganizationCode.equalsIgnoreCase(gb.getSponsorCode())
									&&
									!fundingOrganizationLabel.equalsIgnoreCase(gb.getSponsorName())) {
								log.info("Updating sponsor Label to : " + gb.getSponsorName());
								sb.setLength(0);
								sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
								sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> \n");
								sb.append("DELETE { \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdfs:label ?o . \n");
								sb.append("} \n");
								sb.append("INSERT { \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdfs:label \"" + gb.getSponsorName().trim() + "\" . \n");
								sb.append("} \n");
								sb.append("WHERE { \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdfs:label ?o . \n");
								sb.append("}");
								
								response = vivoClient.vivoUpdateApi(sb.toString());
								log.info(response);
							}
							//Update entire funding organization
							if(!fundingOrganizationCode.equalsIgnoreCase(gb.getSponsorCode())
									&& 
									!fundingOrganizationLabel.equalsIgnoreCase(gb.getSponsorName().trim())) {
								
								log.info("Adding funding organization : " + gb.getSponsorName() + " to grant-" + gb.getAwardNumber().trim());
								sb.setLength(0);
								sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
								sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
								sb.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
								sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
								sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
								sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
								sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
								sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> \n");
								sb.append("DELETE { \n");
								sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:assignedBy <" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> . \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> ?p ?o . \n");
								sb.append("} \n");
								sb.append("INSERT { \n");
								sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:assignedBy <" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> . \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdf:type core:FundingOrganization . \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdfs:label \"" + gb.getSponsorName().trim() + "\" . \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> vitro:mostSpecificType core:FundingOrganization . \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> core:assigns <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> . \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
								sb.append("} \n");
								sb.append("WHERE { \n"); 
								sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:assignedBy <" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> . \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> ?p ?o . \n");
								sb.append("}");
								
								response = vivoClient.vivoUpdateApi(sb.toString());
								log.info(response);
								
								
								sb.setLength(0);
								sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
								sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
								sb.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
								sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
								sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
								sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
								sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
								sb.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> { \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdf:type obo:BFO_0000001 . \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdf:type obo:BFO_0000002 . \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdf:type obo:BFO_0000004 . \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdf:type <http://xmlns.com/foaf/0.1/Organization> . \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdf:type <http://xmlns.com/foaf/0.1/Agent> . \n");
								sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> vitro:mostSpecificType core:FundingOrganization . \n");
								sb.append("}}");
								
								/* response = vivoClient.vivoUpdateApi(sb.toString());
								log.info(response); */ 
							}
						}
					}
				}
				
			} catch(Exception e) {
				log.error("VIVO API Exception" ,e);
			}
		}
		
		/**
		 * This function delete confidential grants from VIVO
		 * @param grants List of current grants from InfoEd	
		 * @param cwid Unique identifier
		 */
		private void deleteConfidentialGrants(List<GrantBean> grants, String cwid) {
			
			SDBJenaConnect vivoJena = null;
			vivoJena = this.jcf.getConnectionfromPool("wcmcCoeus");
			SDBJenaConnect vivoKbInf = this.jcf.getConnectionfromPool("vitro-kb-inf");
			List<String> vivoGrants = new ArrayList<String>();
			List<String> infoEdgrants = grants.stream().map(grant -> grant.getAwardNumber()).collect(Collectors.toList());
			
			
			//get contributor list & date interval for that grant from VIVO
			StringBuilder sb = new StringBuilder();
			sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
			sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
			sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
			sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
			sb.append("select ?grant \n");
			sb.append("from <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> \n");
			sb.append("where { \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> core:relatedBy ?grant . \n");
			sb.append("?grant rdf:type core:Grant . \n");
			sb.append("}");
			
			//log.info(sb.toString());
			
			try{
				ResultSet rs = vivoJena.executeSelectQuery(sb.toString());
				while(rs.hasNext()) {
					QuerySolution qs = rs.nextSolution();
					if(qs.get("grant") != null) {
						vivoGrants.add(qs.get("grant").toString().replace(this.vivoNamespace + "grant-", "").trim());
					}
				}
			} catch(IOException e) {
				log.error("IOException" ,e);
			}
			
			List<String> confidentialGrants = new ArrayList<String>(vivoGrants);
			confidentialGrants.removeAll(infoEdgrants);
			
			if(!confidentialGrants.isEmpty()) {
				
				for(String grantid: confidentialGrants) {
					log.info("Deleting confidential grant : " + grantid);
					Map<String, String> roles = new HashMap<String, String>();
					sb.setLength(0);
					sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
					sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
					sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
					sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
					sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
					sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n");
					sb.append("select distinct ?role ?cwid \n");
					sb.append("from <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> \n");
					sb.append("where { \n");
					sb.append("<" + this.vivoNamespace + "grant-" + grantid.trim() + "> rdf:type core:Grant . \n");
					sb.append("<" + this.vivoNamespace + "grant-" + grantid.trim() + "> core:relates ?role . \n");
					sb.append("?role obo:RO_0000052 ?cwid . \n");
					sb.append("?cwid rdf:type foaf:Person . \n");
					sb.append("FILTER(REGEX(STR(?role),\"http://vivo.med.cornell.edu/individual/role-\")) \n");
					sb.append("}");
					
					try{
						ResultSet rs = vivoJena.executeSelectQuery(sb.toString());
						while(rs.hasNext()) {
							QuerySolution qs = rs.nextSolution();
							if(qs.get("role") != null && qs.get("cwid") != null) {
								roles.put(qs.get("role").toString().trim(), qs.get("cwid").toString().trim());
							}
						}
					} catch(IOException e) {
						log.error("IOException" ,e);
					}
					
					//Delete the grant
					sb.setLength(0);
					
					sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
					sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
					sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
					sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
					sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
					sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> \n");
					sb.append("DELETE { \n");
					sb.append("<" + this.vivoNamespace + "grant-" + grantid.trim() + "> ?p ?o . \n");
					sb.append("<" + this.vivoNamespace + "administrator-role-" + grantid.trim() + "> ?adminPred ?adminObj . \n");
					sb.append("?funding core:assigns <" + this.vivoNamespace + "grant-" + grantid.trim() + "> . \n");
					sb.append("?orgUnit core:relatedBy <" + this.vivoNamespace + "grant-" + grantid.trim() + "> . \n");
					sb.append("?orgUnit obo:RO_0000053 <" + this.vivoNamespace + "administrator-role-" + grantid.trim() + "> . \n");
					sb.append("} WHERE { \n");
					sb.append("<" + this.vivoNamespace + "grant-" + grantid.trim() + "> ?p ?o . \n");
					sb.append("<" + this.vivoNamespace + "administrator-role-" + grantid.trim() + "> ?adminPred ?adminObj . \n");
					sb.append("?funding rdf:type core:FundingOrganization . \n");
					sb.append("?funding core:assigns <" + this.vivoNamespace + "grant-" + grantid.trim() + "> . \n");
					sb.append("?orgUnit rdf:type core:AcademicDepartment . \n");
					sb.append("?orgUnit core:relatedBy <" + this.vivoNamespace + "grant-" + grantid.trim() + "> . \n");
					sb.append("?orgUnit obo:RO_0000053 <" + this.vivoNamespace + "administrator-role-" + grantid.trim() + "> . \n");
					sb.append("}");
					
					try {
						vivoJena.executeUpdateQuery(sb.toString(), true);
						
					} catch(IOException e) {
						log.error("IOException" ,e);
					}
					
					for(Entry<String, String> entry : roles.entrySet()) {
						sb.setLength(0);
						sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> \n");
						sb.append("DELETE { \n");
						sb.append("<" + entry.getKey() + "> ?rolePred ?roleObj . \n");
						sb.append("} WHERE { \n");
						sb.append("<" + entry.getKey() + "> ?rolePred ?roleObj . \n");
						sb.append("}");
						try {
							vivoJena.executeUpdateQuery(sb.toString(), true);
							
						} catch(IOException e) {
							log.error("IOException" ,e);
						}
						
						sb.setLength(0);
						sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
						sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
						sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> \n");
						sb.append("DELETE { \n");
						sb.append("<" + entry.getValue() + "> core:relatedBy <" + this.vivoNamespace + "grant-" + grantid.trim() + "> . \n");
						sb.append("<" + entry.getValue() + "> obo:RO_0000053 <" + entry.getKey() + "> . \n");
						sb.append("} WHERE { \n");
						sb.append("<" + entry.getValue() + "> core:relatedBy <" + this.vivoNamespace + "grant-" + grantid.trim() + "> . \n");
						sb.append("<" + entry.getValue() + "> obo:RO_0000053 <" + entry.getKey() + "> . \n");
						sb.append("}");
						try {
							vivoJena.executeUpdateQuery(sb.toString(), true);
							
						} catch(IOException e) {
							log.error("IOException" ,e);
						}
					}
					//Delete from inf graph
					for(Entry<String, String> entry : roles.entrySet()) {
						sb.setLength(0);
						sb.append("WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> \n");
						sb.append("DELETE { \n");
						sb.append("<" + entry.getKey() + "> ?rolePred ?roleObj . \n");
						sb.append("<" + this.vivoNamespace + "grant-" + grantid.trim() + "> ?p ?o .\n");
						sb.append("} WHERE { \n");
						sb.append("OPTIONAL {<" + entry.getKey() + "> ?rolePred ?roleObj . }\n");
						sb.append("OPTIONAL {<" + this.vivoNamespace + "grant-" + grantid.trim() + "> ?p ?o . }\n");
						sb.append("}");
						try {
							vivoKbInf.executeUpdateQuery(sb.toString(), true);
							
						} catch(IOException e) {
							log.error("IOException" ,e);
						}
					}
				}
				
			}
			this.jcf.returnConnectionToPool(vivoKbInf, "vitro-kb-inf");
			this.jcf.returnConnectionToPool(vivoJena, "wcmcCoeus");
		}
		
		
		/**
		 * This function inserts new grants in VIVO
		 * @param gb the grant bean
		 * @param cwid the unique identifier for faculty
		 * @param crudStatus This indicate the status of operation: INSERT or UPDATE
		 */
		private void insertGrantsInVivo(GrantBean gb, String cwid, String crudStatus) {
			
			log.info("Inserting grant-" + gb.getAwardNumber() + " for cwid - " + cwid);
			String beginDate = null;
			String endDate = null;
			DateFormat shortFormat = new SimpleDateFormat("yyyy-MM-dd",Locale.ENGLISH);
			DateFormat mediumFormat = new SimpleDateFormat("dd-MMM-yy",Locale.ENGLISH);
			DateFormat yearFormat = new SimpleDateFormat("yyyy",Locale.ENGLISH);
			String contributor = null;
			String ctype = null;
			StringBuilder sb = new StringBuilder();
			sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
			sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
			sb.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
			sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
			sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
			sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
			sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
			sb.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> { \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + cwid + "> rdf:type <http://xmlns.com/foaf/0.1/Person> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + cwid + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + cwid + "> core:relatedBy <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> . \n");
				Map<String, String> contributors = gb.getContributors();
				Iterator<Entry<String, String>> it = contributors.entrySet().iterator();
				while(it.hasNext()) {
					Entry<String, String> pair = it.next();
					contributor = pair.getKey().toString();
					ctype = pair.getValue().toString();
					
					if(ctype.equals("PrincipalInvestigatorRole")) {
						if(cwid.equals(contributor))  
							sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> obo:RO_0000053 <" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type core:PrincipalInvestigatorRole . \n");
						sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> obo:RO_0000052 <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> vitro:mostSpecificType core:PrincipalInvestigatorRole . \n");
						sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:relatedBy <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> .\n");
						sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");

						//Date Time Interval Section
						beginDate = gb.getBeginDate().trim();
						endDate = gb.getEndDate().trim();
						try {
							if(!beginDate.equals(""))
								beginDate = yearFormat.format(mediumFormat.parse(beginDate));
							if(!endDate.equals(""))
								endDate = yearFormat.format(mediumFormat.parse(endDate));
						} catch(ParseException e) {
							log.error("ParseException", e);
						}
						//Date Time Interval
						sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-role-pi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "dtinterval-role-pi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
						if(!gb.getBeginDate().equals(""))
							sb.append("<" + this.vivoNamespace + "dtinterval-role-pi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-role" + beginDate.trim() + "> . \n");
						
						if(!gb.getEndDate().equals(""))
							sb.append("<" + this.vivoNamespace + "dtinterval-role-pi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-role" + endDate.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "dtinterval-role-pi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						
						//Start Date Section
						if(!gb.getBeginDate().equals("")) {
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "-01-01T00:00:00\" . \n" );
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						}
						//End Date Section
						if(!gb.getEndDate().equals("")) {
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "-01-01T00:00:00\" . \n" );
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						}


					}
					else if(ctype.equals("KeyPersonnelRole")) {
						if(cwid.equals(contributor))
							sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> obo:RO_0000053 <" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type wcmc:KeyPersonnelRole . \n");
						sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> obo:RO_0000052 <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> vitro:mostSpecificType wcmc:KeyPersonnelRole . \n");
						sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:relatedBy <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> .\n");
						sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
						//Date Time Interval Section
						beginDate = gb.getBeginDate().trim();
						endDate = gb.getEndDate().trim();
						try {
							if(!beginDate.equals(""))
								beginDate = yearFormat.format(mediumFormat.parse(beginDate));
							if(!endDate.equals(""))
								endDate = yearFormat.format(mediumFormat.parse(endDate));
						} catch(ParseException e) {
							log.error("ParseException", e);
						}
						//Date Time Interval
						sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-role-kp-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "dtinterval-role-kp-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
						if(!gb.getBeginDate().equals(""))
							sb.append("<" + this.vivoNamespace + "dtinterval-role-kp-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-role" + beginDate.trim() + "> . \n");
						
						if(!gb.getEndDate().equals(""))
							sb.append("<" + this.vivoNamespace + "dtinterval-role-kp-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-role" + endDate.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "dtinterval-role-kp-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						
						//Start Date Section
						if(!gb.getBeginDate().equals("")) {
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "-01-01T00:00:00\" . \n" );
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						}
						//End Date Section
						if(!gb.getEndDate().equals("")) {
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "-01-01T00:00:00\" . \n" );
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						}
					}
					else if(ctype.equals("PrincipalInvestigatorSubawardRole")) {
						if(cwid.equals(contributor))
							sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> obo:RO_0000053 <" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type wcmc:PrincipalInvestigatorSubawardRole . \n");
						sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> obo:RO_0000052 <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> vitro:mostSpecificType wcmc:PrincipalInvestigatorSubawardRole . \n");
						sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:relatedBy <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> .\n");
						sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
						//Date Time Interval Section
						beginDate = gb.getBeginDate().trim();
						endDate = gb.getEndDate().trim();
						try {
							if(!beginDate.equals(""))
								beginDate = yearFormat.format(mediumFormat.parse(beginDate));
							if(!endDate.equals(""))
								endDate = yearFormat.format(mediumFormat.parse(endDate));
						} catch(ParseException e) {
							log.error("ParseException", e);
						}
						//Date Time Interval
						sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-role-pisa-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "dtinterval-role-pisa-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
						if(!gb.getBeginDate().equals(""))
							sb.append("<" + this.vivoNamespace + "dtinterval-role-pisa-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-role" + beginDate.trim() + "> . \n");
						
						if(!gb.getEndDate().equals(""))
							sb.append("<" + this.vivoNamespace + "dtinterval-role-pisa-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-role" + endDate.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "dtinterval-role-pisa-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						
						//Start Date Section
						if(!gb.getBeginDate().equals("")) {
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "-01-01T00:00:00\" . \n" );
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						}
						//End Date Section
						if(!gb.getEndDate().equals("")) {
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "-01-01T00:00:00\" . \n" );
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						}
					}
					else if(ctype.equals("CoPrincipalInvestigatorRole")) {
						if(cwid.equals(contributor))
							sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> obo:RO_0000053 <" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type vivo:CoPrincipalInvestigatorRole . \n");
						sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> obo:RO_0000052 <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> vitro:mostSpecificType vivo:CoPrincipalInvestigatorRole . \n");
						sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:relatedBy <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> .\n");
						sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
						//Date Time Interval Section
						beginDate = gb.getBeginDate().trim();
						endDate = gb.getEndDate().trim();
						try {
							if(!beginDate.equals(""))
								beginDate = yearFormat.format(mediumFormat.parse(beginDate));
							if(!endDate.equals(""))
								endDate = yearFormat.format(mediumFormat.parse(endDate));
						} catch(ParseException e) {
							log.error("ParseException", e);
						}
						//Date Time Interval
						sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-role-copi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "dtinterval-role-copi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
						if(!gb.getBeginDate().equals(""))
							sb.append("<" + this.vivoNamespace + "dtinterval-role-copi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-role" + beginDate.trim() + "> . \n");
						
						if(!gb.getEndDate().equals(""))
							sb.append("<" + this.vivoNamespace + "dtinterval-role-copi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-role" + endDate.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "dtinterval-role-copi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						
						//Start Date Section
						if(!gb.getBeginDate().equals("")) {
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "-01-01T00:00:00\" . \n" );
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						}
						//End Date Section
						if(!gb.getEndDate().equals("")) {
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "-01-01T00:00:00\" . \n" );
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						}
					}
					else if(ctype.equals("CoInvestigatorRole")) {
						if(cwid.equals(contributor))
							sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> obo:RO_0000053 <" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type wcmc:CoInvestigatorRole . \n");
						sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> obo:RO_0000052 <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> vitro:mostSpecificType wcmc:CoInvestigatorRole . \n");
						sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:relatedBy <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> .\n");
						sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
						//Date Time Interval Section
						beginDate = gb.getBeginDate().trim();
						endDate = gb.getEndDate().trim();
						try {
							if(!beginDate.equals(""))
								beginDate = yearFormat.format(mediumFormat.parse(beginDate));
							if(!endDate.equals(""))
								endDate = yearFormat.format(mediumFormat.parse(endDate));
						} catch(ParseException e) {
							log.error("ParseException", e);
						}
						//Date Time Interval
						sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-role-coi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "dtinterval-role-coi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
						if(!gb.getBeginDate().equals(""))
							sb.append("<" + this.vivoNamespace + "dtinterval-role-coi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-role" + beginDate.trim() + "> . \n");
						
						if(!gb.getEndDate().equals(""))
							sb.append("<" + this.vivoNamespace + "dtinterval-role-coi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-role" + endDate.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "dtinterval-role-coi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						
						//Start Date Section
						if(!gb.getBeginDate().equals("")) {
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "-01-01T00:00:00\" . \n" );
							sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						}
						//End Date Section
						if(!gb.getEndDate().equals("")) {
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "-01-01T00:00:00\" . \n" );
							sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						}
					}
					//Add all other contributors
					if(!cwid.equals(contributor)) {
						sb.append("<" + this.vivoNamespace + "cwid-" + contributor + "> core:relatedBy <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "cwid-" + contributor.trim() + "> . \n");
						sb.append("<" + this.vivoNamespace + "cwid-" + contributor + "> rdf:type <http://xmlns.com/foaf/0.1/Person> . \n");
						sb.append("<" + this.vivoNamespace + "cwid-" + contributor + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
						if(ctype.equals("PrincipalInvestigatorRole")) {
							sb.append("<" + this.vivoNamespace + "cwid-" + contributor.trim() + "> obo:RO_0000053 <" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							//Date Time Interval Section
							beginDate = gb.getBeginDate().trim();
							endDate = gb.getEndDate().trim();
							try {
								if(!beginDate.equals(""))
									beginDate = yearFormat.format(mediumFormat.parse(beginDate));
								if(!endDate.equals(""))
									endDate = yearFormat.format(mediumFormat.parse(endDate));
							} catch(ParseException e) {
								log.error("ParseException", e);
							}
							//Date Time Interval
							sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-role-pi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-pi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
							if(!gb.getBeginDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-pi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-role" + beginDate.trim() + "> . \n");
							
							if(!gb.getEndDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-pi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-role" + endDate.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-pi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							
							//Start Date Section
							if(!gb.getBeginDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							//End Date Section
							if(!gb.getEndDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
						}
						else if(ctype.equals("KeyPersonnelRole")) {
							sb.append("<" + this.vivoNamespace + "cwid-" + contributor.trim() + "> obo:RO_0000053 <" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							//Date Time Interval Section
							beginDate = gb.getBeginDate().trim();
							endDate = gb.getEndDate().trim();
							try {
								if(!beginDate.equals(""))
									beginDate = yearFormat.format(mediumFormat.parse(beginDate));
								if(!endDate.equals(""))
									endDate = yearFormat.format(mediumFormat.parse(endDate));
							} catch(ParseException e) {
								log.error("ParseException", e);
							}
							//Date Time Interval
							sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-role-kp-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-kp-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
							if(!gb.getBeginDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-kp-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-role" + beginDate.trim() + "> . \n");
							
							if(!gb.getEndDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-kp-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-role" + endDate.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-kp-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							
							//Start Date Section
							if(!gb.getBeginDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							//End Date Section
							if(!gb.getEndDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
						}
						else if(ctype.equals("PrincipalInvestigatorSubawardRole")) {
							sb.append("<" + this.vivoNamespace + "cwid-" + contributor.trim() + "> obo:RO_0000053 <" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							//Date Time Interval Section
							beginDate = gb.getBeginDate().trim();
							endDate = gb.getEndDate().trim();
							try {
								if(!beginDate.equals(""))
									beginDate = yearFormat.format(mediumFormat.parse(beginDate));
								if(!endDate.equals(""))
									endDate = yearFormat.format(mediumFormat.parse(endDate));
							} catch(ParseException e) {
								log.error("ParseException", e);
							}
							//Date Time Interval
							sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-role-pisa-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-pisa-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
							if(!gb.getBeginDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-pisa-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-role" + beginDate.trim() + "> . \n");
							
							if(!gb.getEndDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-pisa-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-role" + endDate.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-pisa-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							
							//Start Date Section
							if(!gb.getBeginDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							//End Date Section
							if(!gb.getEndDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
						}
						else if(ctype.equals("CoPrincipalInvestigatorRole")) {
							sb.append("<" + this.vivoNamespace + "cwid-" + contributor.trim() + "> obo:RO_0000053 <" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							//Date Time Interval Section
							beginDate = gb.getBeginDate().trim();
							endDate = gb.getEndDate().trim();
							try {
								if(!beginDate.equals(""))
									beginDate = yearFormat.format(mediumFormat.parse(beginDate));
								if(!endDate.equals(""))
									endDate = yearFormat.format(mediumFormat.parse(endDate));
							} catch(ParseException e) {
								log.error("ParseException", e);
							}
							//Date Time Interval
							sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-role-copi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-copi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
							if(!gb.getBeginDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-copi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-role" + beginDate.trim() + "> . \n");
							
							if(!gb.getEndDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-copi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-role" + endDate.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-copi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							
							//Start Date Section
							if(!gb.getBeginDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							//End Date Section
							if(!gb.getEndDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
						}
						else if(ctype.equals("CoInvestigatorRole")) {
							sb.append("<" + this.vivoNamespace + "cwid-" + contributor.trim() + "> obo:RO_0000053 <" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> . \n");
							//Date Time Interval Section
							beginDate = gb.getBeginDate().trim();
							endDate = gb.getEndDate().trim();
							try {
								if(!beginDate.equals(""))
									beginDate = yearFormat.format(mediumFormat.parse(beginDate));
								if(!endDate.equals(""))
									endDate = yearFormat.format(mediumFormat.parse(endDate));
							} catch(ParseException e) {
								log.error("ParseException", e);
							}
							//Date Time Interval
							sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-role-coi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-coi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
							if(!gb.getBeginDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-coi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-role" + beginDate.trim() + "> . \n");
							
							if(!gb.getEndDate().equals(""))
								sb.append("<" + this.vivoNamespace + "dtinterval-role-coi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-role" + endDate.trim() + "> . \n");
							sb.append("<" + this.vivoNamespace + "dtinterval-role-coi-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							
							//Start Date Section
							if(!gb.getBeginDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
							//End Date Section
							if(!gb.getEndDate().equals("")) {
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTimePrecision core:yearPrecision . \n");
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "-01-01T00:00:00\" . \n" );
								sb.append("<" + this.vivoNamespace + "date-role" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
							}
						}
					}
						
				}
				
				
				sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> rdf:type core:Relationship . \n");
				sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> rdf:type core:Grant . \n");
				sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> rdf:type core:Agreement . \n");
				sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> rdfs:label \"" + gb.getTitle().trim() + "\" . \n");
				sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> vitro:mostSpecificType core:Grant . \n");
				sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "administrator-role-" + gb.getAwardNumber().trim() + "> . \n");
				sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:DateTimeValue \"" + this.strDate + "\" . \n");
				if(gb.getSponsorAwardNumber() != null) { //This can be null in Coeus
					//for(String awardId: gb.getSponsorAwardNumber()) {
						sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:sponsorAwardId \"" + gb.getSponsorAwardNumber().trim() + "\" . \n");
					//} 
				}
				sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
				sb.append("<" + this.vivoNamespace + "administrator-role-" + gb.getAwardNumber().trim() + "> rdf:type core:AdministratorRole . \n");
				sb.append("<" + this.vivoNamespace + "administrator-role-" + gb.getAwardNumber().trim() + "> obo:RO_0000052 <" + this.vivoNamespace + "org-u" + gb.getDepartment() + "> . \n");
				sb.append("<" + this.vivoNamespace + "administrator-role-" + gb.getAwardNumber().trim() + "> core:relatedBy <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> . \n");
				sb.append("<" + this.vivoNamespace + "administrator-role-" + gb.getAwardNumber().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
				sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "administrator-role-" + gb.getAwardNumber().trim() + "> . \n");
				sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:relates <" + this.vivoNamespace + "org-u" + gb.getDepartment() + "> . \n");
				sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:assignedBy <" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> . \n");
				sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdf:type core:FundingOrganization . \n");
				sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdfs:label \"" + gb.getSponsorName().trim() + "\" . \n");
				sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> vitro:mostSpecificType core:FundingOrganization . \n");
				sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> core:assigns <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> . \n");
				sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
				sb.append("<" + this.vivoNamespace + "org-u" + gb.getDepartment() + "> rdf:type core:AcademicDepartment . \n");
				sb.append("<" + this.vivoNamespace + "org-u" + gb.getDepartment() + "> obo:RO_0000053 <" + this.vivoNamespace + "administrator-role-" + gb.getAwardNumber().trim() + "> . \n");
				sb.append("<" + this.vivoNamespace + "org-u" + gb.getDepartment() + "> core:relatedBy <" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> . \n");
				if(gb.isUnitCodeMissing()) {
					sb.append("<" + this.vivoNamespace + "org-u" + gb.getDepartment() + "> rdfs:label \"" + gb.getDepartmentName() + "\" . \n");
				}
				sb.append("<" + this.vivoNamespace + "org-u" + gb.getDepartment() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
				
				//Date Time Interval Section
				beginDate = gb.getBeginDate().trim();
				endDate = gb.getEndDate().trim();
				try {
					if(!beginDate.equals(""))
						beginDate = shortFormat.format(mediumFormat.parse(beginDate));
					if(!endDate.equals(""))
						endDate = shortFormat.format(mediumFormat.parse(endDate));
				} catch(ParseException e) {
					log.error("ParseException", e);
				}
				//Date Time Interval
				sb.append("<" + this.vivoNamespace + "grant-" + gb.getAwardNumber().trim() + "> core:dateTimeInterval <" + this.vivoNamespace + "dtinterval-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> . \n");
				sb.append("<" + this.vivoNamespace + "dtinterval-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> rdf:type core:DateTimeInterval . \n");
				if(!gb.getBeginDate().equals(""))
					sb.append("<" + this.vivoNamespace + "dtinterval-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:start <" + this.vivoNamespace + "date-" + beginDate.trim() + "> . \n");
				
				if(!gb.getEndDate().equals(""))
					sb.append("<" + this.vivoNamespace + "dtinterval-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> core:end <" + this.vivoNamespace + "date-" + endDate.trim() + "> . \n");
				sb.append("<" + this.vivoNamespace + "dtinterval-" + gb.getBeginDate().trim() + "to" + gb.getEndDate().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
				
				//Start Date Section
				if(!gb.getBeginDate().equals("")) {
					sb.append("<" + this.vivoNamespace + "date-" + beginDate.trim() + "> rdf:type core:DateTimeValue . \n");
					sb.append("<" + this.vivoNamespace + "date-" + beginDate.trim() + "> core:dateTimePrecision core:yearMonthDayPrecision . \n");
					sb.append("<" + this.vivoNamespace + "date-" + beginDate.trim() + "> core:dateTime \"" + beginDate.trim() + "T00:00:00\" . \n" );
					sb.append("<" + this.vivoNamespace + "date-" + beginDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
				}
				//End Date Section
				if(!gb.getEndDate().equals("")) {
					sb.append("<" + this.vivoNamespace + "date-" + endDate.trim() + "> rdf:type core:DateTimeValue . \n");
					sb.append("<" + this.vivoNamespace + "date-" + endDate.trim() + "> core:dateTimePrecision core:yearMonthDayPrecision . \n");
					sb.append("<" + this.vivoNamespace + "date-" + endDate.trim() + "> core:dateTime \"" + endDate.trim() + "T00:00:00\" . \n" );
					sb.append("<" + this.vivoNamespace + "date-" + endDate.trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
				}
			
			sb.append("}}");
			log.info(sb.toString());
			try{
				String response = this.vivoClient.vivoUpdateApi(sb.toString());
				log.info(response);
			} catch(Exception  e) {
				log.info("Api Exception", e);
			}
			//insertInferenceTriples(gb, crudStatus);
			log.info("Successful insertion of grant-" + gb.getAwardNumber() + " for cwid: " + cwid);
		}

		
		/**
		 * This function insert inference triples based on operation
		 * @param gb the grant information for a single grant in bean
		 * @param crudStatus the operation like INSERT or UPDATE
		 */
		private void insertInferenceTriples(GrantBean gb, String crudStatus) {
			StringBuilder sb = new StringBuilder();
			String contributor = null;
			String ctype = null;
			
			sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
			sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
			sb.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
			sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
			sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
			sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
			sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
			sb.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> { \n");
			
			Map<String, String> contributors = gb.getContributors();
			Iterator<Entry<String, String>> it = contributors.entrySet().iterator();
			while(it.hasNext()) {
				Entry<String, String> pair = it.next();
				contributor = pair.getKey().toString();
				ctype = pair.getValue(); 
				if(ctype.equalsIgnoreCase("KeyPersonnelRole")) {
					sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type core:InvestigatorRole . \n");
					sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000002 . \n");
					sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000017 . \n");
					sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000020 .\n");
					sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000023 .\n");
					sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000001 .\n");
					sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type core:ResearcherRole . \n");
					sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
					sb.append("<" + this.vivoNamespace + "role-kp-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> vitro:mostSpecificType wcmc:KeyPersonnelRole . \n");
				}
				else if(ctype.equalsIgnoreCase("PrincipalInvestigatorRole")) {
					sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type core:InvestigatorRole . \n");
					sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000002 . \n");
					sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000017 . \n");
					sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000020 .\n");
					sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000023 .\n");
					sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000001 .\n");
					sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type core:ResearcherRole . \n");
					sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
					sb.append("<" + this.vivoNamespace + "role-pi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> vitro:mostSpecificType core:PrincipalInvestigatorRole . \n");
				}
				else if(ctype.equalsIgnoreCase("PrincipalInvestigatorSubawardRole")) {
					sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type core:InvestigatorRole . \n");
					sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000002 . \n");
					sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000017 . \n");
					sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000020 .\n");
					sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000023 .\n");
					sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000001 .\n");
					sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type core:ResearcherRole . \n");
					sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
					sb.append("<" + this.vivoNamespace + "role-pisa-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> vitro:mostSpecificType wcmc:PrincipalInvestigatorSubawardRole . \n");
				}
				else if(ctype.equalsIgnoreCase("CoPrincipalInvestigatorRole")) {
					sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type core:InvestigatorRole . \n");
					sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000002 . \n");
					sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000017 . \n");
					sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000020 .\n");
					sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000023 .\n");
					sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000001 .\n");
					sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type core:ResearcherRole . \n");
					sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
					sb.append("<" + this.vivoNamespace + "role-copi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> vitro:mostSpecificType vivo:CoPrincipalInvestigatorRole . \n");
				}
				else if(ctype.equalsIgnoreCase("CoInvestigatorRole")) {
					sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type core:InvestigatorRole . \n");
					sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000002 . \n");
					sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000017 . \n");
					sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000020 .\n");
					sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000023 .\n");
					sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type obo:BFO_0000001 .\n");
					sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type core:ResearcherRole . \n");
					sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
					sb.append("<" + this.vivoNamespace + "role-coi-" + gb.getAwardNumber().trim() + "-" + contributor.trim() + "> vitro:mostSpecificType wcmc:CoInvestigatorRole . \n");	
				}
			}
			if(crudStatus.equals("INSERT")) {
				//Funding Organization inference triples
				sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdf:type obo:BFO_0000001 . \n");
				sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdf:type obo:BFO_0000002 . \n");
				sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdf:type obo:BFO_0000004 . \n");
				sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdf:type <http://xmlns.com/foaf/0.1/Organization> . \n");
				sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> rdf:type <http://xmlns.com/foaf/0.1/Agent> . \n");
				sb.append("<" + this.vivoNamespace + "org-f" + gb.getSponsorCode() + "> vitro:mostSpecificType core:FundingOrganization . \n");
				if(gb.isUnitCodeMissing()) {
					sb.append("<" + this.vivoNamespace + "org-u" + gb.getDepartment() + "> rdf:type obo:BFO_0000004 . \n");
					sb.append("<" + this.vivoNamespace + "org-u" + gb.getDepartment() + "> rdf:type obo:BFO_0000001 . \n");
					sb.append("<" + this.vivoNamespace + "org-u" + gb.getDepartment() + "> rdf:type obo:BFO_0000002 . \n");
					sb.append("<" + this.vivoNamespace + "org-u" + gb.getDepartment() + "> rdf:type <http://xmlns.com/foaf/0.1/Organization> . \n");
					sb.append("<" + this.vivoNamespace + "org-u" + gb.getDepartment() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
					sb.append("<" + this.vivoNamespace + "org-u" + gb.getDepartment() + "> rdf:type <http://xmlns.com/foaf/0.1/Agent> . \n");
					sb.append("<" + this.vivoNamespace + "org-u" + gb.getDepartment() + "> vitro:mostSpecificType core:AcademicDepartment . \n");
				}
			}
			sb.append("}}");
			
			log.info("Inserting inference triples for grant-" + gb.getAwardNumber());
			try {
				log.info(this.vivoClient.vivoUpdateApi(sb.toString()));
			} catch(Exception e) {
				log.error("API Exception" ,e);
			}
			
		}
		
		
		/**
		 * This function gets all the grants for coeus for a cwid supplied
		 * @param cwid unique identifier for faculty
		 * @return list of grants
		 */
		private List<GrantBean> getGrantsFromCoeus(String cwid, List<String> people) {
			
			Connection con = mcf.getInfoEdConnectionfromPool("INFOED");
			List<GrantBean> grant = new ArrayList<GrantBean>();
			
			StringBuilder selectQuery = new StringBuilder();
			
			selectQuery.append("select distinct v.CWID,v.Account_Number,x.Award_Number,REPLACE(CONVERT(NVARCHAR, begin_date, 106), ' ', '-') as begin_date,REPLACE(CONVERT(NVARCHAR, end_date, 106), ' ', '-') as end_date,replace(replace(replace(z.proj_title,char(13),' '),char(10),' '),'	','') as proj_title, z.unit_name, z.int_unit_code, z.program_type,z.Orig_Sponsor,");
			selectQuery.append("case when z.Sponsor = z.Orig_Sponsor then null when z.Sponsor != z.Orig_Sponsor then z.Sponsor end as Subward_Sponsor,");
			selectQuery.append("z.spon_code,case when z.Sponsor = z.Orig_Sponsor and z.Primary_PI_Flag = 'Y' then 'PrincipalInvestigatorRole' when z.Sponsor != z.Orig_Sponsor and z.Primary_PI_Flag = 'Y' then 'PrincipalInvestigatorSubawardRole' when z.Role_Category = 'PI' then 'CoPrincipalInvestigatorRole' when z.Role_Category = 'Co-investigator' then 'CoInvestigatorRole' else 'KeyPersonnelRole' end as Role ");
			selectQuery.append("from vivo v left join ");
			selectQuery.append("(select distinct cwid, Account_Number, max(Award_Number) as Award_Number from vivo where program_type <> 'Contract without funding' AND Project_Period_Start IS NOT NULL AND Project_Period_End IS NOT NULL group by cwid, Account_Number) x ");
			selectQuery.append("on x.cwid = v.cwid and x.Account_Number = v.Account_Number left join ");
			selectQuery.append("(select distinct cwid, Account_Number, min(Project_Period_Start) as begin_date from vivo where program_type <> 'Contract without funding' AND Project_Period_Start IS NOT NULL AND Project_Period_End IS NOT NULL group by cwid, Account_Number) y ");
			selectQuery.append("on y.cwid = v.cwid and y.Account_Number = v.Account_Number left join ");
			selectQuery.append("(select distinct cwid, Account_Number, max(Project_Period_End) as end_date, max(Sponsor) as Sponsor, max(Orig_Sponsor) as Orig_Sponsor, max(spon_code) as spon_code, max(proj_title) as proj_title, min(program_type) as program_type, min(unit_name) as unit_name, min(int_unit_code) as int_unit_code, max(Primary_PI_Flag) as Primary_PI_Flag, max(role_category) as Role_Category from vivo  group by cwid, Account_Number) z ");
			selectQuery.append("on z.cwid = v.cwid and z.Account_Number = v.Account_Number ");
			selectQuery.append("where v.cwid is not null and Confidential <> 'Y' and v.unit_name is not null and v.program_type <> 'Contract without funding' AND Project_Period_Start IS NOT NULL AND Project_Period_End IS NOT NULL ");
			selectQuery.append("and v.cwid= '" + cwid + "' order by v.cwid, v.Account_Number");
			
			//log.info(selectQuery.toString());
			
			PreparedStatement ps = null;
			java.sql.ResultSet rs = null;
			try {
				ps = con.prepareStatement(selectQuery.toString());
				rs = ps.executeQuery();
				while(rs.next()) {
					GrantBean gb = new GrantBean();
					
					if(rs.getString(2) != null)
						gb.setAwardNumber(rs.getString(2).trim());
					
					if(rs.getString(3) != null) {
						gb.setSponsorAwardNumber(rs.getString(3));
					}
						
					if(rs.getString(6) != null)
						gb.setTitle(StringEscapeUtils.escapeJava(rs.getString(6)).replace("'", "''").trim());
					
					if(rs.getString(4) != null)
						gb.setBeginDate(rs.getString(4));
					else
						gb.setBeginDate("");
					
					if(rs.getString(5) != null)
						gb.setEndDate(rs.getString(5));
					else
						gb.setEndDate("");
					
					if(rs.getString(7) != null)  {
						String unitCode = null;
						if(rs.getString(8) != null) {
							unitCode = rs.getString(8).trim();
						}
						gb.setDepartment(getDepartmentCode(rs.getString(7).trim(),unitCode, gb));
						gb.setDepartmentName(rs.getString(7).trim());
					}
					
					
					if(rs.getString(10) != null)
						gb.setSponsorName(StringEscapeUtils.escapeJava(rs.getString(10)).replace("'", "''").trim());
					
					if(rs.getString(12) != null)
						gb.setSponsorCode(rs.getString(12).trim());
					
					gb.setContributors(getContributors(gb, gb.getAwardNumber(), con, people));
					

					grant.add(gb);
					
					log.info("Grant - " + gb.toString());
					
					
				}
			}
			catch(SQLException e) {
				log.error("SQLException" , e);
			}
			
			finally {
				try{
					if(ps!=null)
						ps.close();
					if(rs!=null)
						rs.close();
					if(con != null)
						mcf.returnConnectionToPool("INFOED", con);
				}
				catch(Exception e) {
					log.error("Exception",e);
				}
				
			}
			
			
			
			return grant;
		}
		
		/**
		 * This function gets the list of contributors for a grant
		 * @param gb the grant information for a single grant in bean
		 * @param cwid unique identifier for faculty
		 * @return a map of contributors having cwid and contributor type
		 */
		private Map<String, String> getContributors(GrantBean gb, String accountNumber, Connection con, List<String> people) {
			Map<String, String> contributors = new HashMap<String, String>();
			String contributor = null;
			
			StringBuilder selectQuery = new StringBuilder();
			
			selectQuery.append("select distinct v.CWID,v.Account_Number,x.Award_Number,begin_date,end_date,replace(replace(replace(z.proj_title,char(13),' '),char(10),' '),'	','') as proj_title, z.unit_name, z.int_unit_code, z.program_type,z.Orig_Sponsor, \n");
			selectQuery.append("case when z.Sponsor = z.Orig_Sponsor then null when z.Sponsor != z.Orig_Sponsor then z.Sponsor end as Subward_Sponsor, \n");
			selectQuery.append("z.spon_code,case when z.Sponsor = z.Orig_Sponsor and z.Primary_PI_Flag = 'Y' then 'PrincipalInvestigatorRole' when z.Sponsor != z.Orig_Sponsor and z.Primary_PI_Flag = 'Y' then 'PrincipalInvestigatorSubawardRole' when z.Role_Category = 'PI' then 'CoPrincipalInvestigatorRole' when z.Role_Category = 'Co-investigator' then 'CoInvestigatorRole' else 'KeyPersonnelRole' end as Role \n");
			selectQuery.append("from vivo v left join \n");
			selectQuery.append("(select distinct cwid, Account_Number, max(Award_Number) as Award_Number from vivo where program_type <> 'Contract without funding' AND Project_Period_Start IS NOT NULL AND Project_Period_End IS NOT NULL group by cwid, Account_Number) x \n");
			selectQuery.append("on x.cwid = v.cwid and x.Account_Number = v.Account_Number left join \n");
			selectQuery.append("(select distinct cwid, Account_Number, min(Project_Period_Start) as begin_date from vivo where program_type <> 'Contract without funding' AND Project_Period_Start IS NOT NULL AND Project_Period_End IS NOT NULL group by cwid, Account_Number) y \n");
			selectQuery.append("on y.cwid = v.cwid and y.Account_Number = v.Account_Number left join \n");
			selectQuery.append("(select distinct cwid, Account_Number, max(Project_Period_End) as end_date, max(Sponsor) as Sponsor, max(Orig_Sponsor) as Orig_Sponsor, max(spon_code) as spon_code, max(proj_title) as proj_title, min(program_type) as program_type, min(unit_name) as unit_name, min(int_unit_code) as int_unit_code, max(Primary_PI_Flag) as Primary_PI_Flag, max(role_category) as Role_Category from vivo  group by cwid, Account_Number) z \n");
			selectQuery.append("on z.cwid = v.cwid and z.Account_Number = v.Account_Number \n");
			selectQuery.append("where v.cwid is not null and Confidential <> 'Y' and v.unit_name is not null and v.program_type <> 'Contract without funding' AND Project_Period_Start IS NOT NULL AND Project_Period_End IS NOT NULL \n");
			selectQuery.append("and v.Account_Number= '" + accountNumber + "' order by v.cwid, v.Account_Number");

			//log.info(selectQuery.toString());
			PreparedStatement ps = null;
			java.sql.ResultSet rs = null;
			try {
					ps = con.prepareStatement(selectQuery.toString());
					rs = ps.executeQuery();
					while(rs.next()) {
						if(rs.getString(1) != null)
							contributor = rs.getString(1).trim();
						
						if(contributor != null && people.contains(contributor.trim())){
							if(rs.getString(13) != null)
								contributors.put(contributor, rs.getString(13));
						}
						
					}
				}
			catch(SQLException e) {
				log.error("SQLException" , e);
			}
			finally {
				try{
					if(ps!=null)
						ps.close();
					if(rs!=null)
						rs.close();
				}
				catch(Exception e) {
					log.error("Exception",e);
				}
				
			}
			log.info("List of contributors for grant-" + gb.getAwardNumber());
			Iterator<Entry<String, String>> it = contributors.entrySet().iterator();
			while(it.hasNext()) {
				Entry<String, String> pair = it.next();
				log.info("Contributor: " + pair.getKey().toString() + " Type: " + pair.getValue().toString());
				
			}
			
			
			return contributors;
		}
		
		/**
		 * This function gets the department code from VIVO_DB
		 * @param deptName the department name for the grant
		 * @return the deptID
		 */
		private String getDepartmentCode(String deptName, String unitCode, GrantBean gb) {
			Connection con = mcf.getAsmsConnectionfromPool("ASMS");
			String deptId = null;
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
				deptName ="Otorlaryngology - Head and Neck Surgery";
			}
			
			if(deptName.trim().equals("Otolaryngology")) {
				deptName ="Otorlaryngology - Head and Neck Surgery";
			}
			
			/*if(deptName.trim().equals("Integrative Medicine")) {
				deptName ="Complementary and Integrative Medicine";
			}*/
					
			String selectQuery = "SELECT DISTINCT id FROM wcmc_department where TRIM(title) = '" + deptName.replaceAll("'", "''").trim() + "'";
			
			//log.info(selectQuery);
			
				try {
					st = con.createStatement();
					rs = st.executeQuery(selectQuery);
					if(rs!=null) { 
						if(rs.next()){
							deptId = rs.getString(1).trim();
						} else {
							deptId = unitCode;
							gb.setUnitCodeMissing(true);
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
							if(con != null)
								mcf.returnConnectionToPool("ASMS", con);
						} catch(SQLException e) {
							log.error("Error in closing connections:", e);
						}
					}
				
			return deptId;
					
		}
		
}
