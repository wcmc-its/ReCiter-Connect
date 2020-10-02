package org.vivoweb.harvester.ingest;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringEscapeUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import lombok.extern.slf4j.Slf4j;

import com.unboundid.ldap.sdk.SearchResultEntry;

import reciter.connect.beans.vivo.PeopleBean;
import reciter.connect.database.ldap.LDAPConnectionFactory;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;
import reciter.connect.vivo.api.client.VivoClient;
import reciter.connect.vivo.sdb.VivoGraphs;

/**
 * @author szd2013
 * This class fetches active faculty from Enterprise Directory and inserts them in VIVO along with details about their phone number, email, Primary Title etc.
 * Note - The release code in Enterprise Directory has to be public for faculty in order for the profile to be created in VIVO.
 * Future Scope - To include graduate students profile in VIVO.
 */
@Slf4j
@Component
public class AcademicFetchFromED {
	
	private int updateCount = 0;
	
	public static String propertyFilePath;

	@Autowired
	private VivoClient vivoClient;
	
	@Autowired
	private LDAPConnectionFactory lcf;
	
	/**
	 * The default namespace for VIVO
	 */
	private String vivoNamespace = JenaConnectionFactory.nameSpace;
	
	/**
	 * Main method
	 * 
	 * @param args
	 *            command-line arguments
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	
	public Callable<String> getCallable(List<PeopleBean> people) {
        return new Callable<String>() {
            public String call() throws Exception {
                return execute(people);
            }
        };
    }
		
		/**
		 * This is the main execution method of the class
		 */
	public String execute(List<PeopleBean> people) throws IOException {
		StopWatch stopWatch = new StopWatch("People fetch performance");
		stopWatch.start("Person updates");
		int count = 0;
		Iterator<PeopleBean>  it = people.iterator();
		while(it.hasNext()) {
			PeopleBean pb = it.next();
			log.info("################################ " + pb.getCwid() + " - " + pb.getDisplayName() + " - Insert/Update Operation #####################");
			if(!checkPeopleInVivo(pb)) {
				log.info("Person: "+pb.getCwid() + " does not exist in VIVO");
				insertPeopleInVivo(pb);
				count = count + 1;
			}
			else {
				log.info("Person: "+pb.getCwid() + " already exist in VIVO");
				checkForUpdates(pb);
				//syncPersonTypes(pb);
			}
			log.info("################################ End of " + pb.getCwid() + " - " + pb.getDisplayName() + " -  Insert/Update Operation ###################");
		}
		
		log.info("Number of new people inserted in VIVO: " + count);
		
		log.info("Number of people updated in VIVO: " + this.updateCount);
		
		stopWatch.stop();
		log.info("People fetch Time taken: " + stopWatch.getTotalTimeSeconds() + "s");
		return "People fetch completed successfully for cwids: " + people.toString();
	}
		
		
		/**
		 * This function gets active people from Enterprise Directory having personTypeCode as academic
		 */
		public List<PeopleBean> getActivePeopleFromED() {

			List<PeopleBean> people = new ArrayList<>();
			int noCwidCount = 0;
			String filter = "(&(objectClass=eduPerson)(weillCornellEduPersonTypeCode=academic))";
			
			List<SearchResultEntry> results = lcf.searchWithBaseDN(filter,"ou=people,dc=weill,dc=cornell,dc=edu");
			
			if (results != null) {
				for (SearchResultEntry entry : results) {
					if(entry.getAttributeValue("weillCornellEduCWID") == null) {
						noCwidCount = noCwidCount + 1;
						//log.info(entry.getAttributeValue("uid"));
						
					}
					if(entry.getAttributeValue("weillCornellEduCWID") != null) {
						PeopleBean pb = new PeopleBean();
						pb.setCwid(entry.getAttributeValue("weillCornellEduCWID"));
						pb.setDisplayName(StringEscapeUtils.escapeJava(entry.getAttributeValue("displayName")));
						
						pb.setGivenName(StringEscapeUtils.escapeJava(entry.getAttributeValue("givenName")));
						
						if(entry.getAttributeValue("mail")!=null)
							pb.setMail(entry.getAttributeValue("mail"));
						else
							pb.setMail("");
						
						if(entry.getAttributeValue("weillCornellEduWorkingTitle") != null)
							pb.setPrimaryTitle(entry.getAttributeValue("weillCornellEduWorkingTitle"));
						else if(entry.getAttributeValue("weillCornellEduPrimaryTitle")!=null)
							pb.setPrimaryTitle(entry.getAttributeValue("weillCornellEduPrimaryTitle"));
						else
							pb.setPrimaryTitle("");
						
						if(entry.getAttributeValue("weillCornellEduStatus")!=null)
							pb.setStatus(entry.getAttributeValue("weillCornellEduStatus"));
						else
							pb.setStatus("");
						
						if(entry.getAttributeValue("telephoneNumber")!= null)
							pb.setTelephoneNumber(entry.getAttributeValue("telephoneNumber"));
						else
							pb.setTelephoneNumber("");
						
						if(entry.getAttributeValue("weillCornellEduMiddleName")!=null)
							pb.setMiddleName(entry.getAttributeValue("weillCornellEduMiddleName"));
						else
							pb.setMiddleName("");
						
						if(entry.getAttributeValue("sn")!=null)
							pb.setSn(entry.getAttributeValue("sn"));
						else
							pb.setSn("");
						
						if(entry.getAttributeValue("labeledURI;pops") != null)
							pb.setPopsProfile(entry.getAttributeValue("labeledURI;pops"));
						else
							pb.setPopsProfile("");
						
						String personType[] = new String[entry.getAttributeValues("weillCornellEduPersonTypeCode").length];
						personType = entry.getAttributeValues("weillCornellEduPersonTypeCode");
						String ptype = assignVivoPersonType(personType);
						pb.setPersonCode(ptype);
						
						List<String> ptypes = Arrays.asList(personType);
						
						ArrayList<String> nsTypes = determineNsType(ptypes);
						
						pb.setNsTypes(nsTypes);
						log.info(pb.toString());
						for(String s: nsTypes) {
							log.info(s);
						}
						log.info("------------------------------------------------------------------------------------------------------------");
						people.add(pb);
				}
			}
				log.info("Number of people found: " + people.size());
				log.info("No of Records with no CWID: " + noCwidCount);
			}
			else {
				log.info("No results found");
			}
			return people;	
		}
		
		/**
		 * @param pb the people bean with all the data that has to be inserted from ED
		 * The function inserts all the data from ED and converts it into triples and insert into wcmcPeople graph in VIVO
		 */
		private void insertPeopleInVivo(PeopleBean pb) {
			
			String middleName = null;
			String mail = null;
			String phone = null;
			
			
			middleName = pb.getMiddleName();
			mail = pb.getMail();
			phone = pb.getTelephoneNumber();
			
			
			if(pb.getMiddleName() ==null) {
				middleName = "";
			}
			
			if(pb.getMail() ==null) {
				mail = "";
			}
			
			if(pb.getTelephoneNumber() == null) {
				phone = "";
			}
			
			String lastMiddleFirst =  pb.getSn().trim() + ", " + pb.getGivenName().trim()  + " " + middleName;
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date date = new Date();
			String currentDate = sdf.format(date);
			StringBuffer sb = new StringBuffer();
			sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
			sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
			sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
			sb.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
			sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
			sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
			sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
			sb.append("PREFIX vcard: <http://www.w3.org/2006/vcard/ns#> \n");
			sb.append( "PREFIX core: <http://vivoweb.org/ontology/core#> \n");
			sb.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> { \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdf:type foaf:Person . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://xmlns.com/foaf/0.1/Agent> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000001> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000002> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000004> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
			for(String nsType: pb.getNsTypes()) {
				sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdf:type " + nsType + " . \n");
			}
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:personLabel \"" + pb.getDisplayName().trim() + "\" . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:cwid \"" + pb.getCwid().trim() + "\" . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdfs:label \"" + lastMiddleFirst + "\" . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vivo:DateTimeValue \"" + currentDate + "\" . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> obo:ARG_2000028 <" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> . \n");
			if(!phone.equals(""))
				sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:officePhone \"" + phone + "\" . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType <" +pb.getPersonCode().trim() + "> . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> obo:ARG_2000029 <" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> rdf:type vcard:Individual . \n");
			if(!mail.equals(""))
				sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> vcard:hasEmail <" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> vcard:hasName <" + this.vivoNamespace + "hasName-"  + pb.getCwid().trim() + "> . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> vcard:hasTitle <" + this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
			if(!mail.equals("")) {
				sb.append("<" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> rdf:type vcard:Work . \n");
				sb.append("<" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> rdf:type vcard:Email . \n");
				sb.append("<" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> vcard:email \"" + mail + "\" . \n");
				sb.append("<" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
			}
			sb.append("<" + this.vivoNamespace + "hasName-"  + pb.getCwid().trim() + "> rdf:type vcard:Name . \n");
			sb.append("<" + this.vivoNamespace + "hasName-"  + pb.getCwid().trim() + "> vcard:givenName \"" + pb.getGivenName().trim() + "\" . \n");
			if(!middleName.equals(""))
				sb.append("<" + this.vivoNamespace + "hasName-"  + pb.getCwid().trim() + "> core:middleName \"" + middleName + "\" . \n");
			sb.append("<" + this.vivoNamespace + "hasName-"  + pb.getCwid().trim() + "> vcard:familyName \"" + pb.getSn().trim() + "\" . \n");
			sb.append("<" + this.vivoNamespace + "hasName-"  + pb.getCwid().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
			sb.append("<" + this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Title . \n");
			sb.append("<" + this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> vcard:title \"" + pb.getPrimaryTitle().trim() + "\" . \n");
			sb.append("<" + this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
			if(!pb.getPopsProfile().equals("")) {
				sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> vcard:hasURL <" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> . \n");
				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> rdf:type vcard:URL . \n");
				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> core:rank \"99\"^^<http://www.w3.org/2001/XMLSchema#int> . \n");
				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> rdfs:label \"Clinical Profile \" . \n");
				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> vcard:url \"" + pb.getPopsProfile().trim() + "\"^^<http://www.w3.org/2001/XMLSchema#anyURI> . \n");
				
			}
			sb.append("}}");
			
			log.info(sb.toString());
			try{
				String response = this.vivoClient.vivoUpdateApi(sb.toString());
				log.info(response);
			} catch(Exception  e) {
				log.info("Api Exception", e);
			}

			
			//insertInferenceTriples(pb);
		}
		
		/**
		 * @param pb the people bean with all the data that has to be inserted from ED
		 * This function inserts inference triples for the people data
		 */
		private void insertInferenceTriples(PeopleBean pb) {
			StringBuilder sb = new StringBuilder();
			sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
			sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
			sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
			sb.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
			sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
			sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
			sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
			sb.append("PREFIX vcard: <http://www.w3.org/2006/vcard/ns#> \n");
			sb.append( "PREFIX core: <http://vivoweb.org/ontology/core#> \n");
			sb.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> { \n");
			//Title Inference Triples
			/*sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Geo . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Explanatory . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Geographical . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Addressing . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Thing . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Communication . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Calendar . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Identification . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Organizational . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:TimeZone . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Security . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> vitro:mostSpecificType vcard:Title . \n");*/
			//For email and primary title
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> rdf:type obo:BFO_0000002 . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> rdf:type obo:BFO_0000031 . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> rdf:type obo:ARG_2000379 . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> rdf:type vcard:Kind . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> rdf:type obo:BFO_0000001 . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> rdf:type obo:IAO_0000030 . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> vitro:mostSpecificType vcard:Individual . \n");
			sb.append("}}");
			
			try{
				String response = this.vivoClient.vivoUpdateApi(sb.toString());
				log.info(response);
			} catch(Exception  e) {
				log.info("Api Exception", e);
			}
		}
		
		/**
		 * This function determine the vivo rdf:types for all the person types coming from ED
		 * @param type the list of person types from ED
		 * @return The list of vivo equivalent rdf:types 
		 */
		private ArrayList<String> determineNsType(List<String> type) {
			ArrayList<String> ptype = new ArrayList<String>();
			
			
			if(type != null && type.contains("academic-faculty-weillfulltime")) {
				ptype.add("wcmc:FullTimeWCMCFaculty");
				
	       }
	        if(type != null && type.contains("academic-faculty-weillparttime")) {
				ptype.add("wcmc:PartTimeWCMCFaculty");
				
			}
	        if(type != null && type.contains("academic-faculty-voluntary")) {
				ptype.add("wcmc:VoluntaryFaculty");
				
			}
	        if(type != null && type.contains("academic-faculty-adjunct")) {
				ptype.add("wcmc:AdjunctFaculty");
				
			}
	        if(type != null && type.contains("academic-faculty-courtesy")) {
				ptype.add("wcmc:CourtesyFaculty");
				
			}
	        if(type != null && type.contains("academic-faculty-emeritus")) {
				ptype.add("core:EmeritusFaculty");
				
			}
	        if(type != null && type.contains("academic-faculty-instructor")) {
				ptype.add("wcmc:Instructor");
				
			}
	        if(type != null && type.contains("academic-faculty-lecturer")) {
				ptype.add("wcmc:Lecturer");
				
			}
	        if(type != null && type.contains("academic-nonfaculty-fellow")) {
				ptype.add("wcmc:Fellow");
				
			}
	        if(type != null && type.contains("academic-nonfaculty-postdoc")) {
				ptype.add("core:Postdoc");
				
			}
	        if(type != null && type.contains("academic-faculty")) {
				ptype.add("vivo:FacultyMember");
				
			}
			 if(type != null && type.contains("academic-nonfaculty")) {
				ptype.add("core:NonAcademic");
				
			}
			
			//ptype.remove(mostSpecificType);
			
			return ptype;
		}
		
		private String assignVivoPersonType(String[] personType) {
			String ptype = null;
			String type = null;
			
	                for(String s: personType) {
	                	type = type + " " + s;
	                }
	                type = type.replace("null", "");
	       
	       if(type != null && type.contains("academic-faculty-weillfulltime")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#FullTimeWCMCFaculty";
				return ptype;
	       }
	       else if(type != null && type.contains("academic-faculty-weillparttime")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#PartTimeWCMCFaculty";
				return ptype;
			}
	       else if(type != null && type.contains("academic-faculty-voluntary")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#VoluntaryFaculty";
				return ptype;
			}
	       else if(type != null && type.contains("academic-faculty-adjunct")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#AdjunctFaculty";
				return ptype;
			}
	       else if(type != null && type.contains("academic-faculty-courtesy")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#CourtesyFaculty";
				return ptype;
			}
	       else if(type != null && type.contains("academic-faculty-emeritus")) {
				ptype = "http://vivoweb.org/ontology/core#EmeritusFaculty";
				return ptype;
			}
	       else if(type != null && type.contains("academic-faculty-instructor")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#Instructor";
				return ptype;
			}
	       else if(type != null && type.contains("academic-faculty-lecturer")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#Lecturer";
				return ptype;
			}
	       else if(type != null && type.contains("academic-nonfaculty-fellow")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#Fellow";
				return ptype;
			}
	       else if(type != null && type.contains("academic-nonfaculty-postdoc")) {
				ptype = "http://vivoweb.org/ontology/core#Postdoc";
				return ptype;
			}
	       else if(type != null && type.contains("academic-faculty")) {
				ptype = "http://vivoweb.org/ontology/core#FacultyMember";
				return ptype;
			}
			else if(type != null && type.contains("academic-nonfaculty")) {
				ptype = "http://vivoweb.org/ontology/core#NonAcademic";
				return ptype;
			}
			
			
			return ptype;

		}
		
		/**
		 * @param pb  the people bean with all the data that has to be inserted from ED
		 * @return true or false based on whether the people exist in VIVO
		 * @throws IOException exception likely to be thrown by SDBJenaConnect
		 */
		private boolean checkPeopleInVivo(PeopleBean pb) throws IOException {
			int count = 0;
			String sparqlQuery = "PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
									"PREFIX foaf:     <http://xmlns.com/foaf/0.1/> \n" +
									"SELECT  (count(rdf:type) as ?c) \n" +
									"WHERE {\n" +
									"GRAPH <" + VivoGraphs.PEOPLE_GRAPH + "> {" +
									"<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdf:type foaf:Person . \n" +
									"}}";

			log.info(sparqlQuery);

			try {
				String response = this.vivoClient.vivoQueryApi(sparqlQuery);
				log.info(response);
				JSONObject obj = new JSONObject(response);
				JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
				count = bindings.getJSONObject(0).getJSONObject("c").getInt("value");
			} catch(Exception e) {
				log.error("Api Exception", e);
			}
			
			if(count > 0)
				return true;
			else
				return false;			
		}
		
		/**
		 * @param pb the people bean with all the data that has to be inserted from ED
		 * This function check for updates from ED and then apply them in VIVO. For email , displayName, lastName, Phone numbers, Primary Title etc.
		 */
		private void checkForUpdates(PeopleBean pb) {
			ArrayList<String> updateList = new ArrayList<String>();
			ArrayList<String> insertList = new ArrayList<String>();
			String phone = null;
			String middleName = null;
			String label = null;
			String type = null;
			String title = null;
			String email = null;
			String firstName = null;
			String lastName = null;
			String popsUrl = null;
			String sparqlQuery = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
					"PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n" +
					"PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n" +
					"PREFIX core: <http://vivoweb.org/ontology/core#> \n" +
					"PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n" +
					"PREFIX vcard: <http://www.w3.org/2006/vcard/ns#> \n" +
					"SELECT ?label ?type ?phone ?title ?email ?firstName ?lastName ?middleName ?popsUrl\n" +
					//"FROM <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> \n" +
					"WHERE \n" +
					"{ \n" +
					"GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> {\n" +
					"<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:personLabel ?label .\n" +
					"<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType ?type .\n" +
					"<" + this.vivoNamespace + "hasTitle-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#title> ?title . \n" +
					"<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#givenName> ?firstName . \n" +
					"<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#familyName> ?lastName . \n" +
					"OPTIONAL { <" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:officePhone ?phone . }\n" +
					"OPTIONAL { <" + this.vivoNamespace + "hasEmail-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#email> ?email . }\n" +
					"OPTIONAL { <" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> core:middleName ?middleName . }\n" +
					"OPTIONAL { <" + this.vivoNamespace + "popsUrl-" + pb.getCwid().trim() + "> vcard:url ?popsUrl } \n" +
					"}}";
			
			log.debug(sparqlQuery);
			try {
				String response = this.vivoClient.vivoQueryApi(sparqlQuery);
				log.info(response);
				JSONObject obj = new JSONObject(response);
				JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
				if(bindings != null && !bindings.isEmpty()) {
					if(bindings.getJSONObject(0).optJSONObject("label").has("value"))
						label = bindings.getJSONObject(0).getJSONObject("label").getString("value");
					
					if(bindings.getJSONObject(0).optJSONObject("type").has("value"))
						type = bindings.getJSONObject(0).getJSONObject("type").getString("value");
					
					if(bindings.getJSONObject(0).optJSONObject("phone") != null && bindings.getJSONObject(0).optJSONObject("phone").has("value"))
						phone = bindings.getJSONObject(0).getJSONObject("phone").getString("value");
					
					if(bindings.getJSONObject(0).optJSONObject("title") != null && bindings.getJSONObject(0).optJSONObject("title").has("value"))
						title = bindings.getJSONObject(0).getJSONObject("title").getString("value");
					
					if(bindings.getJSONObject(0).optJSONObject("email") !=  null && bindings.getJSONObject(0).optJSONObject("email").has("value"))
						email = bindings.getJSONObject(0).getJSONObject("email").getString("value");
					
					if(bindings.getJSONObject(0).optJSONObject("firstName").has("value"))
						firstName = bindings.getJSONObject(0).getJSONObject("firstName").getString("value");

					if(bindings.getJSONObject(0).optJSONObject("lastName").has("value"))
						lastName = bindings.getJSONObject(0).getJSONObject("lastName").getString("value");
					
					if(bindings.getJSONObject(0).optJSONObject("middleName") != null && bindings.getJSONObject(0).optJSONObject("middleName").has("value"))
						middleName = bindings.getJSONObject(0).getJSONObject("middleName").getString("value");
					
					if(bindings.getJSONObject(0).optJSONObject("popsUrl") != null && bindings.getJSONObject(0).optJSONObject("popsUrl").has("value"))
						popsUrl = bindings.getJSONObject(0).getJSONObject("popsUrl").getString("value");

				
					if(label != null && !label.equals(pb.getDisplayName().trim())) {
						updateList.add("DisplayName");
						log.info("Person Label was updated for cwid: " + pb.getCwid().trim());
					}
					if(type != null && !type.equals(pb.getPersonCode().trim())) {
						updateList.add("MostSpecificType");
						log.info("MostSpecificType was updated for cwid: " + pb.getCwid().trim());
					}
					if(title != null && !title.equals(pb.getPrimaryTitle().trim())) {
						updateList.add("PrimaryTitle");
						log.info("Title was updated for cwid: " + pb.getCwid().trim());
					}
					if(email != null && !email.equals(pb.getMail().trim())) {
						updateList.add("Mail");
						log.info("Email was updated for cwid: " + pb.getCwid().trim());
					}
					
					if(email == null && pb.getMail() != null && !pb.getMail().equals("")) {
						insertList.add("Mail");
						log.info("Email was inserted for cwid: " + pb.getCwid().trim());
					}
					
					if(phone == null && pb.getTelephoneNumber() != null && !pb.getTelephoneNumber().equals("")) {
						insertList.add("TelephoneNumber");
						log.info("Phone was inserted for cwid: " + pb.getCwid().trim());
					}
					
					if(middleName == null && pb.getMiddleName() != null && !pb.getMiddleName().equals("")) {
						insertList.add("MiddleName");
						log.info("Middle Name was inserted for cwid: " + pb.getCwid().trim());
					}
					
					if(phone != null && !phone.equals(pb.getTelephoneNumber().trim())) {
						updateList.add("TelephoneNumber");
						log.info("Phone was updated for cwid: " + pb.getCwid().trim());
					}
					if(firstName != null && !firstName.equals(pb.getGivenName().trim())) {
						updateList.add("FirstName");
						log.info("First was updated for cwid: " + pb.getCwid().trim());
					}
					if(lastName != null && !lastName.equals(pb.getSn().trim())) {
						updateList.add("LastName");
						log.info("Last Name was updated for cwid: " + pb.getCwid().trim());
					}
					if(middleName!= null && !middleName.equals(pb.getMiddleName().trim())) {
						updateList.add("MiddleName");
						log.info("Middle Name was updated for cwid: " + pb.getCwid().trim());
					}
					if(popsUrl == null && pb.getPopsProfile() != null && !pb.getPopsProfile().equals("")) {
						insertList.add("PopsUrl");
						log.info("Pops Url was inserted for cwid: " + pb.getCwid().trim());
					}
				}
					
					if(updateList.isEmpty()) {
						log.info("No Updates are necessary for cwid : " + pb.getCwid().trim());
					}
					else {
						StringBuffer sb = new StringBuffer();
						
						sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
						sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n");
		                sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
		                sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
		                sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
		                sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> \n");
		                sb.append("DELETE { \n");
		                if(updateList.contains("DisplayName")) {
		                    sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:personLabel \"" + label + "\" .\n");
		                }
		                if(updateList.contains("PrimaryTitle")) {
		                	sb.append("<" + this.vivoNamespace + "hasTitle-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#title> \"" + title + "\" .\n");
		                }
		                if(updateList.contains("Mail")) {
		                	sb.append("<" + this.vivoNamespace + "hasEmail-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#email> \"" + email + "\" .\n");
		                }
		                if(updateList.contains("TelephoneNumber")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:officePhone \"" + phone + "\" .\n");
		                }
		                if(updateList.contains("FirstName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#givenName> \"" + firstName + "\" .\n");
		                }
		                if(updateList.contains("LastName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#familyName> \"" + lastName + "\" .\n");
		                }
		                if(updateList.contains("MiddleName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> core:middleName \"" + middleName + "\" .\n");
		                }
		                if(updateList.contains("MostSpecificType")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType <" + type + "> .\n");
		                }
		                sb.append("} \n");
		                sb.append("INSERT { \n");
		                if(updateList.contains("DisplayName")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:personLabel \"" + pb.getDisplayName().trim() + "\" .\n");
		                }
		                if(updateList.contains("PrimaryTitle")) {
		                	sb.append("<" + this.vivoNamespace + "hasTitle-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#title> \"" + pb.getPrimaryTitle().trim() + "\" .\n");
		                }
		                if(updateList.contains("Mail")) {
		                	sb.append("<" + this.vivoNamespace + "hasEmail-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#email> \"" + pb.getMail().trim() + "\" .\n");
		                }
		                if(updateList.contains("TelephoneNumber")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:officePhone \"" + pb.getTelephoneNumber().trim() + "\" .\n");
		                }
		                if(updateList.contains("FirstName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#givenName> \"" + pb.getGivenName().trim() + "\" .\n");
		                }
		                if(updateList.contains("LastName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#familyName> \"" + pb.getSn().trim() + "\" .\n");
		                }
		                if(updateList.contains("MiddleName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> core:middleName \"" + pb.getMiddleName().trim() + "\" .\n");
		                }
		                
		                if(updateList.contains("MostSpecificType")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType <" + pb.getPersonCode().trim() + "> .\n");
		                }
		                
		                sb.append("} \n");
		                sb.append("WHERE { \n");
		                if(updateList.contains("DisplayName")) {
		                    sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:personLabel \"" + label + "\" .\n");
		                }
		                if(updateList.contains("PrimaryTitle")) {
		                	sb.append("<" + this.vivoNamespace + "hasTitle-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#title> \"" + title + "\" .\n");
		                }
		                if(updateList.contains("Mail")) {
		                	sb.append("<" + this.vivoNamespace + "hasEmail-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#email> \"" + email + "\" .\n");
		                }
		                if(updateList.contains("TelephoneNumber")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:officePhone \"" + phone + "\" .\n");
		                }
		                if(updateList.contains("FirstName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#givenName> \"" + firstName + "\" .\n");
		                }
		                if(updateList.contains("LastName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#familyName> \"" + lastName + "\" .\n");
		                }
		                if(updateList.contains("MiddleName")) {
		                	sb.append("OPTIONAL { <" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> core:middleName \"" + middleName + "\" . }\n");
		                }
		                if(updateList.contains("MostSpecificType")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType <" + type + "> .\n");
		                }
		                sb.append("} \n");
		                
		                log.info("Update Query: " + sb.toString());
		                log.info(this.vivoClient.vivoUpdateApi(sb.toString()));
		                
		                this.updateCount = this.updateCount + 1;
						if(!updateList.isEmpty() && updateList.contains("MostSpecificType")) {
							log.info("Updating inference triple for mostSpecificType update");
							sb.setLength(0);
							
							sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
							sb.append("WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> \n");
			                sb.append("DELETE { \n");
			                sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType ?o .\n");
			                sb.append("} \n");
			                sb.append("INSERT { \n");
			                sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType <" + pb.getPersonCode().trim() + "> .\n");
			                sb.append("} \n");
			                sb.append("WHERE { \n");
			                sb.append("OPTIONAL {<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType ?o . }\n");
			                sb.append("}");
			                
			                log.info("Update Query for person type: " + sb.toString());
			                log.info(this.vivoClient.vivoUpdateApi(sb.toString()));
						}
					}
					
					if(!insertList.isEmpty()) {
	                	StringBuilder sb = new StringBuilder();
						
						sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
						sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n");
		                sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
		                sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
		                sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
		                sb.append("PREFIX vcard: <http://www.w3.org/2006/vcard/ns#> \n");
		                sb.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> { \n");
		                if(insertList.contains("TelephoneNumber")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:officePhone \"" + pb.getTelephoneNumber().trim() + "\" .\n");			                	
		                }
		                if(insertList.contains("MiddleName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> core:middleName \"" + pb.getMiddleName().trim() + "\" .\n");
		                }
		                if(insertList.contains("Mail")) {
		                	sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> vcard:hasEmail <" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> . \n");
		                	sb.append("<" + this.vivoNamespace + "hasEmail-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#email> \"" + pb.getMail().trim() + "\" .\n");
		                	sb.append("<" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> rdf:type vcard:Work . \n");
		    				sb.append("<" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> rdf:type vcard:Email . \n");
		    				sb.append("<" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
		                }
		                if(insertList.contains("PopsUrl")) {
		                	if(!pb.getPopsProfile().equals("")) {
		        				sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> vcard:hasURL <" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> . \n");
		        				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> rdf:type vcard:URL . \n");
		        				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> core:rank \"99\"^^<http://www.w3.org/2001/XMLSchema#int> . \n");
		        				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> rdfs:label \"Clinical Profile \" . \n");
		        				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> vcard:url \"" + pb.getPopsProfile().trim() + "\"^^<http://www.w3.org/2001/XMLSchema#anyURI> . \n");
		        				
		        			}
		                }
		                sb.append("}}");
		                
						log.info("Insert Query: " + sb.toString());
						log.info(this.vivoClient.vivoUpdateApi(sb.toString()));
						//Run inferencing on the updated triples
						//insertInferenceTriples(pb);
	                }
					
			} catch(Exception e) {
				log.error("Api Exception" ,e);
			}
			
			
		}
		
		/**
		 * This function will sync all the personTypeCodes from ED with VIVO rdf types
		 * @param pb The PeopleBean conatining all people information from ED
		 */
		private void syncPersonTypes(PeopleBean pb) {
			
			log.info("Syncing personTypeCodes from ED with VIVO for " + pb.getCwid());
			StringBuilder sb = new StringBuilder();
			
			sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
			sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
			sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
			sb.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
			sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
			sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
			sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
			sb.append("PREFIX vcard: <http://www.w3.org/2006/vcard/ns#> \n");
			sb.append( "PREFIX core: <http://vivoweb.org/ontology/core#> \n");
			sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> \n");
            sb.append("DELETE { \n");
            sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdf:type ?o .\n");
            sb.append("} \n");
            sb.append("INSERT { \n");
            sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdf:type foaf:Person . \n");
            sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://xmlns.com/foaf/0.1/Agent> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000001> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000002> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000004> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
			for(String nsType: pb.getNsTypes()) {
				sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdf:type " + nsType + " . \n");
			}
            sb.append("} \n");
            sb.append("WHERE { \n");
            sb.append("OPTIONAL {<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdf:type ?o . }\n");
            sb.append("}");
			
            log.info(sb.toString());
			try {
				log.info(this.vivoClient.vivoUpdateApi(sb.toString()));
			}catch(Exception e) {
				log.error("Api Exception", e);
			}
		}
}
