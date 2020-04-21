package org.vivoweb.harvester.ingest;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.vivoweb.harvester.publication.workflow.PublicationAuthorshipMaintenance;
import org.vivoweb.harvester.publication.workflow.ScopusInProcessUpdate;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;

import reciter.connect.beans.vivo.*;
import reciter.connect.database.mysql.MysqlConnectionFactory;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;

/**
 * @author szd2013
 * <p><b><i> This class fetches publications from PubAdmin and checks against VIVO whether they exist or not and inserts them in VIVO.
 * It will check for updates for citation count, publication type. </i></b></p>
 */
public class PublicationFetch {
	/**
	 * <i>This is the global connection variable for all connections to PubAdmin</i>
	 */
	private Connection con = null;
	
	/**
	 * <i> Connection factory object to get connections to PubAdmin </i>
	 */
	@Autowired
	private MysqlConnectionFactory mcf;
	
	/**
	 * <i>Jena connection factory object for all the apache jena sdb related connections</i>
	 */
	@Autowired
	private JenaConnectionFactory jcf;
	
	/**
	 * The default namespace for VIVO
	 */
	private String vivoNamespace = JenaConnectionFactory.nameSpace;
	
	/**
	 * <i>This is the logger variable for all logging</i>
	 */
	private static Logger log = LoggerFactory.getLogger(PublicationFetch.class);
	
	
	/**
	 * This is the collection of active people in ED
	 */
	private List<String> activePeople = new ArrayList<String>();
	
	/**
	 * This is the scopusApikey used to access the Scopus(elsevier) API
	 */
	private static String scopusApiKey = null;
	
	/**
	 * This is the insttoken used to access the Scopus(elsevier) API
	 */
	private static String scopusInsttoken = null;
	
	/**
	 * This is the return type of the API for Scopus(elsevier) API
	 */
	private static String scopusAccept = null;
	
	/**
	 * This gives the count of publication which was newly added to VIVO
	 */
	private int newPubCount = 0 ;
	
	/**
	 * This gives the count of existing publication which was updated in VIVO
	 */
	private int updatePubCount = 0;
	 
	/**
	 * This gives the total count of publications where citiation count was updated in VIVO and Pubadmin
	 */
	private int citeCountPubs = 0;
	
	/**
	 * This gives the total count of publications where In process publication was assigned updated PubTypes from updstream SCOPUS and PUBMED count was updated in VIVO and Pubadmin
	 */
	private int inProcessPubsCount = 0 ;
	
	/**
	 * This is the updateFlag which is set when anything is updated in VIVO
	 */
	private boolean updateFlag = false;
	
	/**
	 * This is the InsertQuery String builder for publication
	 */
	public StringBuilder sbInsert = new StringBuilder();
	
	/**
	 * This is the UpdateQuery String builder for publication
	 */
	public StringBuilder sbUpdate = new StringBuilder();
	
	/**
	 * This is the InferenceQuery String builder for publication
	 */
	public StringBuilder inf = new StringBuilder();
	
	/**
	 * The list of VIVO authorships for a publication
	 */
	public List<PublicationAuthorshipMaintenanceBean> vivoAuthorships = new ArrayList<PublicationAuthorshipMaintenanceBean>();
	
	/**
	 * The global SDBJenaConnect reference for all Jena related connection
	 */
	public SDBJenaConnect vivoJena = null;
	
	private Map<String, String> journalIdMap = new HashMap<String,String>();
	
	private Set<String> meshMajor = new HashSet<String>();
	
	private Set<String> pubType = new HashSet<String>();
	
	private Date currentDate = new Date();
	
	private Calendar cal = Calendar.getInstance();

	@Autowired
	private EdDataInterface edi;
	
	
	/**
	 * Main method
	 * 
	 * @param args
	 *            command-line arguments
	 */
	/* public static void main (String args[]) {
		
		if (args.length == 0) {
			log.info("Usage: java fetch.JSONPeopleFetch [properties filename]");
			log.info("e.g. java fetch.JSONPeopleFetch /usr/share/vivo-ed-people/examples/wcmc_people.properties");
		} else if (args.length == 1) { // path of the properties file
			propertyFilePath = args[0];
			
			Properties props = new Properties();
			try {
				props.load(new FileInputStream(propertyFilePath.trim()));
			} catch (FileNotFoundException fe) {
				log.info("File not found error: " + fe);
			} catch (IOException e) {
				log.info("IOException error: " + e);
			}
			
			scopusApiKey = props.getProperty("scopusApiKey").trim();
			scopusAccept = props.getProperty("scopusApiAccept").trim();
			scopusInsttoken = props.getProperty("scopusApiInsttoken").trim();
			
			
			new PublicationFetch().execute();
		}
	} */
	
	/**
	 * This is the main execution method of the class
	 */
	public void execute() {
		if(this.vivoNamespace == null) {
			log.info("Please provide a namespace in property file");
		}
		else {
			this.con = this.mcf.getConnectionfromPool();
			this.activePeople = edi.getPeopleInVivo(this.jcf);
			PublicationBean pb = new PublicationBean();
			Set<AuthorBean> authors = new HashSet<AuthorBean>();
			Iterator<String> it = this.activePeople.iterator();
			while(it.hasNext()) {
				String cwid = it.next();
			log.info("#############################################Publication Fetch Start for " + cwid + "############################################################");
				fetchPublications(cwid.trim(), pb, authors);
			log.info("#############################################Publication Fetch End for " + cwid + "############################################################");
			}
			
			
			log.info("Total number of new publications added to VIVO: " + this.newPubCount);
			log.info("Total number of publications updated in VIVO: " + this.updatePubCount);
			log.info("Total number of publication updated with new citation count in VIVO: " + this.citeCountPubs);
			log.info("Total number of in process publication updated with publication type in VIVO: " + this.inProcessPubsCount);
			
			
			
			//Destory Mysql connection pool
			if(this.con!=null) {
				this.mcf.returnConnectionToPool(this.con);
			}
		}
	}
	
	/**
	 * <p><b><i>This method fetches all the valid publications and their relevant fields from PubAdmin and stores them in a PublicationBean type Set <p><b><i>
	 * @param cwid <i> This is the center wide unique identifier</i>
	 * @param pb PublicationBean containing all information
	 * @param authors Set of AuthorBean with all author information
	 */
	private void fetchPublications(String cwid, PublicationBean pb, Set<AuthorBean> authors) {
		
		log.info("***********************Workflow Start for a Publication**********************");
		String selectQuery = "SELECT distinct wcmc_document_pk, scopus_doc_id, pmid, pmcid, doi, publication_name, cover_date, pages, volume, issue, citation_count, issn, eissn, \n" +
							 "nlmabbreviation, abstract, mesh_major, language, funding, pubtype, isbn10, isbn13, title, pubmed_xml_content, \n" +
							 "case when issn is not null and eissn is not null then 1 \n" +
							 "when issn is not null and eissn is null then 2 \n" +
							 "when issn is null and eissn is not null then 3 \n" +
							 "when issn is null and eissn is null then 4 \n" +
							 "end as journal_order \n" +
							 "FROM wcmc_authorship INNER JOIN wcmc_document_authorship ON wcmc_authorship_fk = wcmc_authorship_pk INNER JOIN wcmc_document ON wcmc_document_fk = wcmc_document_pk \n" +
							 "WHERE (wcmc_document.pubtype not like '%Retract%' or wcmc_document.pubtype not like '%rratum%' or wcmc_document.pubtype is not null) and (wcmc_document.duplicate = 'N' or wcmc_document.duplicate is null or (wcmc_document.duplicate = 'Y' and wcmc_document.duplicate_override ='Y')) "
							 + "and wcmc_authorship.cwid = '" + cwid.trim() + "' order by journal_order ASC , wcmc_document.pubtype ASC";
		
		log.info(selectQuery);
		PreparedStatement ps = null;
		java.sql.ResultSet rs = null;
		try {
				ps = this.con.prepareStatement(selectQuery);
				rs = ps.executeQuery();
				while(rs.next()) {

					if(rs.getString(1) != null && !rs.getString(1).isEmpty())
						pb.setPublicationId(Integer.parseInt(rs.getString(1)));
					
					if(rs.getString(2) != null && !rs.getString(2).isEmpty())
						pb.setScopusDocId(rs.getString(2).trim());
					else 
						pb.setScopusDocId("null");
				
					
					if(rs.getString(3) != null && !rs.getString(3).isEmpty())
						pb.setPmid(Integer.parseInt(rs.getString(3).trim()));
					else
						pb.setPmid(0);
					
					if(rs.getString(4) != null && !rs.getString(4).isEmpty())
						pb.setPmcid(rs.getString(4).trim());
					else
						pb.setPmcid("null");
					
					if(rs.getString(5) != null && !rs.getString(5).isEmpty())
						pb.setDoi(rs.getString(5).trim().replaceAll("([\\\\\\\\\"])", "\\\\$1").replaceAll("\\s+", "")); //replace whitespace and any escape back slashes
					else
						pb.setDoi("null");
					
					if(rs.getString(6) != null && !rs.getString(6).isEmpty())
						pb.setJournal(rs.getString(6).trim().replaceAll("([\\\\\\\\\"])", "\\\\$1"));
					else
						pb.setJournal("null");
					
					
					
					if(rs.getString(7) != null && !rs.getString(7).isEmpty())
						pb.setCoverDate(rs.getString(7).trim());
					else
						pb.setCoverDate("null");
					
					if(rs.getString(8) != null && !rs.getString(8).isEmpty())
						pb.setPages(rs.getString(8).trim());
					else
						pb.setPages("null");
					
					if(rs.getString(9) != null && !rs.getString(9).isEmpty())
						pb.setVolume(rs.getString(9).trim());
					else
						pb.setVolume("null");
					
					if(rs.getString(10) != null && !rs.getString(10).isEmpty())
						pb.setIssue(rs.getString(10).trim());
					else
						pb.setIssue("null");
					
					if(rs.getString(11) != null && !rs.getString(11).isEmpty())
						pb.setCitationCount(Integer.parseInt(rs.getString(11).trim()));
					else
						pb.setCitationCount(-1);
					
					
					if(rs.getString(12) != null && !rs.getString(12).isEmpty() && rs.getString(12).trim().length() >= 8) {
						pb.setIssn(rs.getString(12).trim().replaceAll("\\s+", ""));
						if(pb.getIssn().length() == 8 && !pb.getIssn().contains("-"))
							pb.setIssn(new StringBuilder(pb.getIssn()).insert(4, "-").toString());
					}
					else
						pb.setIssn("null");
					
					if(rs.getString(13) != null && !rs.getString(13).isEmpty() && rs.getString(13).trim().length() >= 8) {
						pb.setEissn(rs.getString(13).trim().replaceAll("\\s+", ""));
						if(pb.getEissn().length() == 8 && !pb.getEissn().contains("-") )
							pb.setEissn(new StringBuilder(pb.getEissn()).insert(4, "-").toString());
					}
					else
						pb.setEissn("null");
					
					if(rs.getString(14) != null && !rs.getString(14).isEmpty())
						pb.setNlmabbreviation(rs.getString(14).trim());
					else
						pb.setNlmabbreviation("null");
					
					if(rs.getString(15) != null && !rs.getString(15).isEmpty())
						pb.setPublicationAbstract(rs.getString(15).trim().replaceAll("([\\\\\\\\\"])", "\\\\$1"));
					else
						pb.setPublicationAbstract("null");
					
					//log.info(StringEscapeUtils.escapeJava(new String(IOUtils.toByteArray(rs.getAsciiStream(15)),"UTF-8")));
					
					
					//log.info(rs.getString(22).trim().replaceAll("([\\\\\\\\\"\\[\\{\\(\\*\\+\\?\\^\\$\\|‌​])", "\\\\$1"));
					
					//log.info(rs.getString(22).trim().replaceAll("([\\\\\\\\\"\\[\\{\\(])", "\\\\$1"));
					
					
					
					if(rs.getString(16) != null && !rs.getString(16).isEmpty()) {
						String[] mesh = rs.getString(16).trim().split("\\|");
						//Set<String> meshMajor = new HashSet<String>();
						if(!this.meshMajor.isEmpty()) {
							this.meshMajor.clear();
						}
						for(String m: mesh) {
							this.meshMajor.add(m);
						}
						pb.setMeshMajor(this.meshMajor);
					}
					
					if(rs.getString(17) != null && !rs.getString(17).isEmpty())
						pb.setLanguage(rs.getString(17).trim());
					else
						pb.setLanguage("null");
					
					/*if(rs.getString(18) != null && !rs.getString(18).isEmpty()) {
						String[] funding = rs.getString(18).split("\\|");
						Set<String> fundings = new HashSet<String>();
						for(String f: funding) {
							fundings.add(f);
						}
						pb.setFunding(fundings);
					}*/
					
					if(rs.getString(19) != null && !rs.getString(19).isEmpty()) {
						//Set<String> pubType = new HashSet<String>();
						String[] ptype = rs.getString(19).trim().split("\\|");
						if(!this.pubType.isEmpty()) {
							this.pubType.clear();
						}
						for(String p: ptype) {
							this.pubType.add(p);
						}
						pb.setPubtype(this.pubType);
					}
					
					if(rs.getString(20) != null && !rs.getString(20).isEmpty())
						pb.setIsbn10(rs.getString(20).trim().replaceAll("\\s+", ""));
					else
						pb.setIsbn10("null");
					
					if(rs.getString(21) != null && !rs.getString(21).isEmpty())
						pb.setIsbn13(rs.getString(21).trim().replaceAll("\\s+", ""));
					else
						pb.setIsbn13("null");
					
					if(rs.getString(22) != null && !rs.getString(22).isEmpty()) {
						if(rs.getString(22).trim().startsWith("Errat"))
							pb.setTitle("null");
						else
							pb.setTitle(rs.getString(22).trim().replaceAll("([\\\\\\\\\"])", "\\\\$1"));
					}
					else
						pb.setTitle("null");
					
					//log.info(StringEscapeUtils.escapeJava(new String(IOUtils.toByteArray(rs.getAsciiStream(22)),"UTF-8")));
					
					if(rs.getString(23) != null && !rs.getString(23).isEmpty()) {
						pb.setPubmedXmlContent(rs.getString(23));
					}
					else
						pb.setPubmedXmlContent("null");

					if(!pb.getScopusDocId().equals("null"))
						pb.setAuthorList(getAuthors(pb.getPublicationId(), authors));
					//else
						//pb.setAuthorList(null);
					
					pb.printPublication();
					
					//Update workflow starts here 
					if(checkPublicationExist(pb)) {
						log.info("Publication - pubid" + pb.getScopusDocId() + " already exist in VIVO. Checking for updates.");
						//Check if a publication has citationCount and go to scopus to see if that count changed or not 
						//Only run citation count update on weekends
						this.cal.setTime(this.currentDate);
						if(this.cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || this.cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
							citationCountUpdate(pb);
						else
							log.info("Skipping Citation Count updates for publications since its not the weekend");
						//Check for change in authorships and assign correct authorship
						syncAuthorship(pb);
						
						//Update in process publication from Scopus and Pubmed if it has changed in VIVO
						if(pb.getVivoPubTypeDeduction().contains("InProcess"))
							updateInProcessPubs(pb);
						
						if(pb.getPubtypeStr() != null && pb.getPubtypeStr().trim().equals("ch")) {
							log.info("Checking for links from chapter to books - ");
							linkBookToChapters(pb);
						}
						
						if(pb.getPubtypeStr() != null && !pb.getPubtypeStr().trim().equals("ch") && !pb.getPubtypeStr().trim().equals("bk")) {
							log.info("Checking for journal assignment mismatch - ");
							//Journal Sync module is disabled as this is not required. if there is discrepancy in journal tagging to publications you may enable the module.
							//journalSync(pb);
						}
						
						
						
						//Update Pubadmin with update date
						if(this.updateFlag) {
							updatePubAdmin(pb, "UPDATE");
							this.updatePubCount++;
						}
						
						this.updateFlag = false;
						
						
					}
					else { //Insert publication into VIVO as it does not exist in VIVO
						if(!pb.getTitle().equals("null")) { //Ignore Erratum publication
							importPublication(pb);
							this.newPubCount++;
						}
						else
							log.info("Erratum Title - Publication will not be imported");
						
					}
					
					log.info("***********************Workflow End for a Publication**********************");
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
			
			
	}
	
	/**
	 * <p><b><i>This function gets the list of authors for a specified scopus document id</p></b></i>
	 * @param docPk <p><i>This is the primary key of wcmc_document table</p></i>
	 * @param authors The list of authors for a publication
	 * @return <p><i>Set of Authors for a specific publication</p></i>
	 */
	@SuppressWarnings("boxing")
	private Set<AuthorBean> getAuthors(int docPk, Set<AuthorBean> authors) {
		
		if(authors != null && !authors.isEmpty())
			authors.clear();

		String selectQuery = "SELECT cwid, authname, surname, given_name, initials, scopus_author_id, wcmc_authorship_rank, wcmc_authorship.wcmc_authorship_pk \n" +
							 "FROM wcmc_authorship \n" + 
							 "INNER JOIN wcmc_document_authorship ON wcmc_authorship_fk = wcmc_authorship_pk \n" +
							 "INNER JOIN wcmc_document ON wcmc_document_fk = wcmc_document_pk \n" + 
							 "WHERE wcmc_document.wcmc_document_pk = " + docPk + " \n" +
							 "AND wcmc_document_authorship.ignore_flag is null \n" +
							 "GROUP BY cwid, authname, surname, given_name, initials, scopus_author_id \n" +
							 "ORDER BY wcmc_document_authorship.wcmc_authorship_rank";
		//log.info(selectQuery);
		
		PreparedStatement ps = null;
		java.sql.ResultSet rs = null;
		try {
				ps = this.con.prepareStatement(selectQuery);
				rs = ps.executeQuery();
				while(rs.next()) {
					AuthorBean ab = new AuthorBean();
					if(rs.getString(1) != null && !rs.getString(1).trim().isEmpty()) 
						ab.setCwid(rs.getString(1));
					else
						ab.setCwid("null");
					
					if(rs.getString(2) != null && !rs.getString(2).trim().isEmpty())
						ab.setAuthName(rs.getString(2));
					else
						ab.setAuthName("null");
					
					if(rs.getString(3) != null && !rs.getString(3).trim().isEmpty())
						ab.setSurname(rs.getString(3));
					else
						ab.setSurname("null");
					
					if(rs.getString(4) != null && !rs.getString(4).trim().isEmpty())
						ab.setGivenName(rs.getString(4));
					else
						ab.setGivenName("null");
					
					if(rs.getString(5) != null && !rs.getString(5).trim().isEmpty())
						ab.setInitials(rs.getString(5));
					else
						ab.setInitials("null");
					
					if(rs.getString(6) != null && !rs.getString(6).trim().isEmpty())
						ab.setScopusAuthorId(Long.parseLong(rs.getString(6).trim()));
					else
						ab.setScopusAuthorId((long)0);
					
					if(rs.getString(7) != null && !rs.getString(7).trim().isEmpty())
						ab.setAuthorshipRank(Integer.parseInt(rs.getString(7).trim()));
					else
						ab.setAuthorshipRank(0);
					
					int authorshipPk = rs.getInt(8);
					if(!rs.wasNull() && authorshipPk != 0) //primitive type null check with wasNull and  will retrieve 0 if its actually 0
						ab.setAuthorshipPk(authorshipPk);
					
					authors.add(ab);
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
		return authors;
	}
	
	/**
	 * <p><b><i> This function imports the publication to VIVO using Apache SDB Jena.
	 * @param pb This is the publication bean which has the full publication details
	 */
	private void importPublication(PublicationBean pb) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		String strDate = sdf.format(this.currentDate);
		String crudType = "INSERT";
		
		if(this.sbInsert.length() > 0)
			this.sbInsert.setLength(0);
		
		if(this.inf.length() > 0)
			this.inf.setLength(0);
				
		//StringBuilder this.inf = new StringBuilder(); //This is the query builder for inference triple
		this.inf.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
		this.inf.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
		this.inf.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
		this.inf.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
		this.inf.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
		this.inf.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
		this.inf.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
		this.inf.append("PREFIX bibo: <http://purl.org/ontology/bibo/> \n");
		this.inf.append("PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \n");
		this.inf.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> { \n");
		
		
		//StringBuilder this.sbInsert = new StringBuilder();
		this.sbInsert.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
		this.sbInsert.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
		this.sbInsert.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
		this.sbInsert.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
		this.sbInsert.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
		this.sbInsert.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
		this.sbInsert.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
		this.sbInsert.append("PREFIX bibo: <http://purl.org/ontology/bibo/> \n");
		this.sbInsert.append("PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \n");
		this.sbInsert.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> { \n");
		this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> core:DateTimeValue \"" + strDate + "\" . \n");
		this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type obo:BFO_0000001 . \n");
		this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type obo:BFO_0000002 . \n");
		this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type obo:IAO_0000030 . \n");
		this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type obo:BFO_0000031 . \n");
		
		//CitationCount
		if(pb.getCitationCount() > 0 && pb.getCitationCount()!= -1) {
			this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> <http://purl.org/spar/c4o/hasGlobalCitationFrequency> <" + this.vivoNamespace + "citation-pubid" + pb.getScopusDocId().trim() + ">  . \n");
			this.sbInsert.append("<" + this.vivoNamespace + "citation-pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://purl.org/spar/c4o/GlobalCitationCount> . \n");
			this.sbInsert.append("<" + this.vivoNamespace + "citation-pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
			this.sbInsert.append("<" + this.vivoNamespace + "citation-pubid" + pb.getScopusDocId().trim() + "> rdfs:label \"" + pb.getCitationCount() + "\" . \n");
			
		}
		
		//Determine publication type
		determinePublicationType(pb);
		
		//freetext
		this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> core:freetextKeyword \"" + determineFreetextKeyword(pb.getVivoPubTypeDeduction()) + "\". \n");
		//doi
		if(!pb.getDoi().equals("null") && !pb.getDoi().isEmpty())
			this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> bibo:doi \"" + pb.getDoi().trim() + "\" . \n");
		//ScopusDocId
		if(!pb.getScopusDocId().equals("null") && !pb.getScopusDocId().isEmpty())
			this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> wcmc:scopusDocId \"" + pb.getScopusDocId().trim() + "\" . \n");
		//pmid
		if(pb.getPmid() != 0)
			this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> bibo:pmid \"" + pb.getPmid() + "\" . \n");
		//pmcid
		if(!pb.getPmcid().equals("null"))
			this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> core:pmcid \"" + pb.getPmcid().trim() + "\" . \n");
		//language
		if(!pb.getLanguage().equals("null") && !pb.getLanguage().isEmpty())
			this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> wcmc:language \"" + pb.getLanguage().trim() + "\" . \n");
		//page start & page end - Split by first occurence of `-`
		if(!pb.getPages().equals("null") && !pb.getPages().isEmpty()) {
			if(pb.getPages().contains("-")) {
				String[] pages = pb.getPages().trim().split("-", 2);
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> bibo:pageStart \"" + pages[0].trim() + "\" . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> bibo:pageEnd \"" + pages[1].trim() + "\" . \n");
			}
			else
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> bibo:pageStart \"" + pb.getPages().trim() + "\" . \n");
			
		}
		//Issue
		if(!pb.getIssue().equals("null") && !pb.getIssue().isEmpty())
			this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> bibo:number \"" + pb.getIssue().trim() + "\" . \n");
		
		//Volume
		if(!pb.getVolume().equals("null") && !pb.getVolume().isEmpty())
			this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> bibo:volume \"" + pb.getVolume().trim() + "\" . \n");
		//MeshMajor
		if(pb.getMeshMajor() != null && !pb.getMeshMajor().isEmpty()) {
			for(String meshM: pb.getMeshMajor()) {
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> wcmc:hasMeshMajor <" + this.vivoNamespace + "concept" + meshM.trim().replaceAll("[^a-zA-Z0-9]", "") + "> . \n");
				if(checkMeshMajorExists(meshM.trim().replaceAll("[^a-zA-Z0-9]", ""))) {
					this.sbInsert.append("<" + this.vivoNamespace + "concept" + meshM.trim().replaceAll("[^a-zA-Z0-9]", "") + "> wcmc:meshMajorFor <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> . \n");
				}
				else {
					this.sbInsert.append("<" + this.vivoNamespace + "concept" + meshM.trim().replaceAll("[^a-zA-Z0-9]", "") + "> rdf:type <http://www.w3.org/2004/02/skos/core#Concept> . \n");
					this.sbInsert.append("<" + this.vivoNamespace + "concept" + meshM.trim().replaceAll("[^a-zA-Z0-9]", "") + "> vitro:mostSpecificType <http://www.w3.org/2004/02/skos/core#Concept> . \n");
					this.sbInsert.append("<" + this.vivoNamespace + "concept" + meshM.trim().replaceAll("[^a-zA-Z0-9]", "") + "> rdfs:label \"" + meshM.trim() + "\" . \n");
					this.sbInsert.append("<" + this.vivoNamespace + "concept" + meshM.trim().replaceAll("[^a-zA-Z0-9]", "") + "> wcmc:meshMajorFor <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> . \n");
					
				}
			}
		}
		//CoverDate
		if(!pb.getCoverDate().equals("null") && !pb.getCoverDate().isEmpty()) {
			Date coverDate = null;
			try {
         	   coverDate = sdf.parse(pb.getCoverDate().trim());
         	   
            } catch(ParseException e) {
         	   log.error("ParseException", e);
            }
			
			this.cal.setTime(coverDate);
			int month = this.cal.get(Calendar.MONTH)+1; // Since calender month start with 0
			
			if(pb.getCoverDate().endsWith("-01") && coverDate != null) { // case for monthYear
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> core:dateTimeValue <" + this.vivoNamespace + "monthyear" + (month<10?("0"+month):(month)) + this.cal.get(Calendar.YEAR) + "> . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "monthyear" + (month<10?("0"+month):(month)) + this.cal.get(Calendar.YEAR) + "> rdf:type core:DateTimeValue . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "monthyear" + (month<10?("0"+month):(month)) + this.cal.get(Calendar.YEAR) + "> vitro:mostSpecificType core:DateTimeValue. \n");
				this.sbInsert.append("<" + this.vivoNamespace + "monthyear" + (month<10?("0"+month):(month)) + this.cal.get(Calendar.YEAR) + "> core:dateTimePrecision core:yearMonthPrecision . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "monthyear" + (month<10?("0"+month):(month)) + this.cal.get(Calendar.YEAR) + "> core:dateTime \"" + pb.getCoverDate().trim() + "T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . \n");
			}
			else { //case for daymonthyear
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> core:dateTimeValue <" + this.vivoNamespace + "daymonthyear" + this.cal.get(Calendar.DAY_OF_MONTH) + (month<10?("0"+month):(month)) + this.cal.get(Calendar.YEAR) + "> . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "daymonthyear" + this.cal.get(Calendar.DAY_OF_MONTH) + (month<10?("0"+month):(month)) + this.cal.get(Calendar.YEAR) + "> rdf:type core:DateTimeValue . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "daymonthyear" + this.cal.get(Calendar.DAY_OF_MONTH) + (month<10?("0"+month):(month)) + this.cal.get(Calendar.YEAR) + "> vitro:mostSpecificType core:DateTimeValue. \n");
				this.sbInsert.append("<" + this.vivoNamespace + "daymonthyear" + this.cal.get(Calendar.DAY_OF_MONTH) + (month<10?("0"+month):(month)) + this.cal.get(Calendar.YEAR) + "> core:dateTimePrecision core:yearMonthDayPrecision . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "daymonthyear" + this.cal.get(Calendar.DAY_OF_MONTH) + (month<10?("0"+month):(month)) + this.cal.get(Calendar.YEAR) + "> core:dateTime \"" + pb.getCoverDate().trim() + "T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . \n");
			}
		}
		//Label
		if(!pb.getTitle().equals("null") && !pb.getTitle().isEmpty()) {
			this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdfs:label \"" + pb.getTitle().trim() + "\" . \n");
		}
		//Abstract
		if(pb.getPublicationAbstract() != null && !pb.getPublicationAbstract().equals("null") && !pb.getPublicationAbstract().isEmpty()) {
			this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> bibo:abstract \"" + pb.getPublicationAbstract().trim() + "\" . \n");
		}
		//Journal
		if(!pb.getJournal().equals("null") && !pb.getJournal().isEmpty()) {
			if(!pb.getVivoPubTypeDeduction().equals("book") && !pb.getVivoPubTypeDeduction().equals("book-chapter")) {
				if(!checkJournalExistsInVivo(pb)) {
					log.info("Checking if journal exist in VIVO - " + pb.getJournal());
				}
			}
		}
		//Authorship
		if(!pb.getAuthorList().isEmpty()) {
			for(AuthorBean ab: pb.getAuthorList()) {
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> core:relatedBy <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> rdf:type obo:BFO_0000001 . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> rdf:type obo:BFO_0000002 . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> rdf:type obo:BFO_0000020 . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> rdf:type core:Relationship . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> rdf:type core:Authorship . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> vitro:mostSpecificType core:Authorship . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> core:relates <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> core:rank \"" + ab.getAuthorshipRank() + "\"^^xsd:integer . \n");
				//Linking vcard of the person
				if(!ab.getCwid().isEmpty() && !ab.getCwid().equals("null") && this.activePeople.contains(ab.getCwid().trim())) {
					this.sbInsert.append("<" + this.vivoNamespace + "cwid-" + ab.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> . \n");
					this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> core:relates <" + this.vivoNamespace + "cwid-" + ab.getCwid().trim() + "> . \n");
					this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> core:relates <" + this.vivoNamespace + "arg2000028-" + ab.getCwid().trim() + "> . \n");
					this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getCwid().trim() + "> obo:ARG_2000029 <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> . \n");
					this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getCwid().trim() + "> core:relatedBy <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> . \n");
					
					
					//Inference Triples
					this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> obo:ARG_2000028 <" + this.vivoNamespace + "arg2000028-" + ab.getCwid().trim() + "> . \n");
					
				}
				else {
					if(checkExternalAuthorExists(ab)) {
						log.info("External Authorship exist for author: " + ab.getAuthName() + " with rank " + ab.getAuthorshipRank() + " - person" + ab.getAuthorshipPk());
						this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> core:relates <" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> core:relatedBy <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> core:relates <" + this.vivoNamespace + "person" + ab.getAuthorshipPk() + "> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "person" + ab.getAuthorshipPk() + "> core:relatedBy <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> obo:ARG_2000029 <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> . \n");
					}
					else {
						//create the external author
						//int randomNumber = generateRandomNumber();
						log.info("External Authorship does not exist for author: " + ab.getAuthName() + " with rank " + ab.getAuthorshipRank() + " - person" + ab.getAuthorshipPk());
						this.sbInsert.append("<" + this.vivoNamespace + "person" + ab.getAuthorshipPk() +"> rdf:type <http://xmlns.com/foaf/0.1/Agent> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "person" + ab.getAuthorshipPk() +"> rdf:type wcmc:ExternalEntity . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "person" + ab.getAuthorshipPk() +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000001> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "person" + ab.getAuthorshipPk() +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000002> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "person" + ab.getAuthorshipPk() +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000004> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "person" + ab.getAuthorshipPk() +"> rdf:type <http://xmlns.com/foaf/0.1/Person> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "person" + ab.getAuthorshipPk() +"> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "person" + ab.getAuthorshipPk() +"> vitro:mostSpecificType wcmc:ExternalEntity . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "person" + ab.getAuthorshipPk() +"> <http://purl.obolibrary.org/obo/ARG_2000028> <" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "person" + ab.getAuthorshipPk() +"> vivo:relatedBy <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "person" + ab.getAuthorshipPk() +"> <http://www.w3.org/2000/01/rdf-schema#label> \"" + ab.getSurname().trim() + ", " + ab.getGivenName().trim() + "\" . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> obo:BFO_0000002 . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> obo:BFO_0000031 . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> obo:BFO_0000001 . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> obo:ARG_2000379 . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Kind> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> obo:IAO_0000030 . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Thing> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Individual> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> vitro:mostSpecificType <http://www.w3.org/2006/vcard/ns#Individual> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> <http://purl.obolibrary.org/obo/ARG_2000029> <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> <http://www.w3.org/2006/vcard/ns#hasName> <" + this.vivoNamespace + "hasName-person" + ab.getAuthorshipPk() +"> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> <http://www.w3.org/2006/vcard/ns#hasTitle> <" + this.vivoNamespace + "hasTitle-person" + ab.getAuthorshipPk() +"> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "hasName-person" + ab.getAuthorshipPk() + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Name> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "hasName-person" + ab.getAuthorshipPk() + "> <http://www.w3.org/2006/vcard/ns#givenName> \"" + ab.getGivenName().trim() + "\" . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "hasName-person" + ab.getAuthorshipPk() + "> <http://www.w3.org/2006/vcard/ns#familyName> \"" + ab.getSurname().trim() + "\" . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "hasTitle-person" + ab.getAuthorshipPk() + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Title> . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "hasTitle-person" + ab.getAuthorshipPk() + "> <http://www.w3.org/2006/vcard/ns#title> \"External Author\" . \n");
						this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> <http://vivoweb.org/ontology/core#relates> <" + this.vivoNamespace + "person" + ab.getAuthorshipPk() +"> .\n");
						this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> <http://vivoweb.org/ontology/core#relates> <" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> .\n");
						this.sbInsert.append("<" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> core:relatedBy <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> . \n");
						//Inference Triples
						this.inf.append("<" + this.vivoNamespace + "person" + ab.getAuthorshipPk() +"> rdf:type wcmc:ExternalEntity . \n");
					}
					//Inference Triples
					this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> obo:ARG_2000028 <" + this.vivoNamespace + "arg2000028-" + ab.getAuthorshipPk() + "> . \n");
				}
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"Scopus-Pubmed-Harvester\" . \n");
				
			}
		}
		//Harvested By
		this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"Scopus-Pubmed-Harvester\" . \n");

		this.sbInsert.append("}}");
		
		this.inf.append("}}");
		
		//log.info(IOUtils.toString(new ByteArrayInputStream(this.sbInsert.toString().getBytes(StandardCharsets.UTF_8)), "UTF-8"));
		
		if(log.isDebugEnabled())
			log.debug("Insert Query " + this.sbInsert.toString());
		
		if(log.isDebugEnabled())
			log.debug("Inference Triple Insert Query " + this.inf.toString());
		
		
		this.vivoJena = this.jcf.getConnectionfromPool("wcmcPublications");
		try {
			this.vivoJena.executeUpdateQuery(this.sbInsert.toString(), true);
		} catch(IOException e) {
			log.error("Error connecting to SDBJena");
		}
		this.jcf.returnConnectionToPool(this.vivoJena, "wcmcPublications");
		
		log.info("Inserting inference Triples for pubid" + pb.getScopusDocId());
		
		
		this.vivoJena = this.jcf.getConnectionfromPool("vitro-kb-inf");
		
		try {
			this.vivoJena.executeUpdateQuery(this.inf.toString(), true);
		} catch(IOException e) {
			log.error("Error connecting to SDBJena");
		}
		this.jcf.returnConnectionToPool(this.vivoJena, "vitro-kb-inf");
		
		updatePubAdmin(pb, crudType);
		
	}
	
	/**
	 * This function determine publication types based on the record was pulled from scopus only or from scopus and pubmed both
	 * @param pb PublicationBean containing all the publication information 
	 * @return Reference to the query builder
	 */
	private String determinePublicationType(PublicationBean pb) {
		
		boolean onlyScopus = true;
		//Check for documents that are indexed in both pubmed and scopus
		if(pb.getPmid() != 0 && !pb.getScopusDocId().equals("null")) {
			//Editorials
			if(pb.getPubtypeStr().trim().contains("Editorial")) {
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Article . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:EditorialArticle . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType vivo:EditorialArticle . \n");
				pb.setVivoPubTypeDeduction("editorial");
				
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType vivo:EditorialArticle . \n");
			}
			//Letters
			else if(pb.getPubtypeStr().trim().contains("Letter")) {
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Article . \n");
				//this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:PersonalCommunicationDocument . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type wcmc:Letter . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType wcmc:Letter . \n");
				pb.setVivoPubTypeDeduction("letter");
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType wcmc:Letter . \n");
			}
			//Comments
			else if(pb.getPubtypeStr().trim().contains("Comment")) {
				//this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Article . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type wcmc:Comment . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType wcmc:Comment . \n");
				pb.setVivoPubTypeDeduction("comment");
				
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType wcmc:Comment . \n");
				
			}
			//Conference Papers
			else if(pb.getPubtypeStr().contains("Consensus Development Conference") || pb.getPubtypeStr().contains("addresses") || pb.getPubtypeStr().contains("clinical conference") ||
				 pb.getPubtypeStr().contains("congresses") || pb.getPubtypeStr().contains("lectures") || ( pb.getJournal().contains("proceedings") && ( pb.getJournal().contains("20") || pb.getJournal().contains("19"))) ||
				 (pb.getJournal().contains("proceedings") && pb.getJournal().contains("symposi")) || (pb.getJournal().contains("proceedings") && pb.getJournal().contains("congress")) ||
				 pb.getJournal().contains("conference") || pb.getJournal().contains("workshop") || pb.getJournal().contains("colloqui") || pb.getJournal().contains("meeting")) {
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Article . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type core:ConferencePaper . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType core:ConferencePaper . \n");
				pb.setVivoPubTypeDeduction("conference");
				
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType core:ConferencePaper . \n");
			}
			//Review Articles
			else if(pb.getPubtypeStr().trim().contains("Meta-Analysis") || pb.getPubtypeStr().trim().contains("Review") || pb.getPubtypeStr().trim().contains("Classical Article") || pb.getPubtypeStr().trim().contains("Scientific Integrity Review") || pb.getPubtypeStr().trim().contains("Guideline") || pb.getPubtypeStr().trim().contains("Practice Guideline")) {
				//this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Article . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:Review . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType vivo:Review . \n");
				pb.setVivoPubTypeDeduction("review");
				
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType vivo:Review . \n");
			}
			//Academic Article
			else if(pb.getPubtypeStr().trim().contains("Journal Article") || pb.getPubtypeStr().contains("Clinical Trial") || pb.getPubtypeStr().contains("Clinical Trial, Phase I") || pb.getPubtypeStr().contains("Clinical Trial, Phase II") || pb.getPubtypeStr().contains("Clinical Trial, Phase III") || pb.getPubtypeStr().contains("Clinical Trial, Phase IV")
				|| pb.getPubtypeStr().contains("Randomized Controlled Trial") || pb.getPubtypeStr().contains("Multicenter Study") || pb.getPubtypeStr().contains("Twin Study") || pb.getPubtypeStr().contains("Validation Studies") || pb.getPubtypeStr().contains("Case Reports") || pb.getPubtypeStr().contains("Comparative Study") || pb.getPubtypeStr().contains("Technical Report")) {
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Article . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:AcademicArticle . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType bibo:AcademicArticle . \n");
				pb.setVivoPubTypeDeduction("academic-article");
				
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType bibo:AcademicArticle . \n");
				
			}
			//Everything else should be tagged to Articles
			else {
				//this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Article . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType bibo:Article . \n");
				pb.setVivoPubTypeDeduction("article");
				
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType bibo:Article . \n");
			}
			onlyScopus = false;
		}
		//else if(pb.getPmid()==0 && !pb.getScopusDocId().equals("null")) {
		//These will be the articles which is only indexed in scopus
			//In Process Articles
			if(pb.getPubtypeStr().trim().equals("ip")) {
				//this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Article . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type wcmc:InProcess . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType wcmc:InProcess . \n");
				pb.setVivoPubTypeDeduction("inprocess");
				
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType wcmc:InProcess . \n");
			}
			//Academic Article
			else if(pb.getPubtypeStr().trim().equals("ar")) {
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Article . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:AcademicArticle . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType bibo:AcademicArticle . \n");
				pb.setVivoPubTypeDeduction("academic-article");
				
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType bibo:AcademicArticle . \n");
			}
			//Conference Papers
			else if(pb.getPubtypeStr().trim().equals("cp")) {
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Article . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type core:ConferencePaper . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType core:ConferencePaper . \n");
				pb.setVivoPubTypeDeduction("conference");
				
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType core:ConferencePaper . \n");
			}
			//Editorials
			else if(pb.getPubtypeStr().trim().equals("ed")) {
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Article . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:EditorialArticle . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType vivo:EditorialArticle . \n");
				pb.setVivoPubTypeDeduction("editorial");
				
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType vivo:EditorialArticle . \n");
			}
			//Letters
			else if(pb.getPubtypeStr().trim().equals("le")) {
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Article . \n");
				//this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:PersonalCommunicationDocument . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type wcmc:Letter . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType wcmc:Letter . \n");
				pb.setVivoPubTypeDeduction("letter");
				
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType wcmc:Letter . \n");
			}
			//Comments
			else if(pb.getPubtypeStr().trim().equals("no")) {
				//this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Article . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type wcmc:Comment . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType wcmc:Comment . \n");
				pb.setVivoPubTypeDeduction("comment");
				
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType wcmc:Comment . \n");
				
			}
			//News Release
			else if(pb.getPubtypeStr().trim().equals("pr")) {
				//this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:NewsRelease . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType vivo:NewsRelease . \n");
				pb.setVivoPubTypeDeduction("press-release");
				
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType vivo:NewsRelease . \n");
				
			}
			//Report
			else if(pb.getPubtypeStr().trim().equals("re")) {
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Report . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType bibo:Report . \n");
				pb.setVivoPubTypeDeduction("report");
				
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType bibo:Report . \n");
			}
			//Book Chapter
			else if(pb.getPubtypeStr().trim().equals("ch")) {
				pb.setVivoPubTypeDeduction("book-chapter");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:DocumentPart . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:BookSection . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Chapter . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType bibo:Chapter . \n");
				checkJournalExistsInVivo(pb);
				
				if(pb.getIsbn13().equals("null") && pb.getIsbn10().equals("null") && !pb.getIssn().equals("null") && !pb.getJournal().equals("null") && !pb.getJournal().isEmpty()) {
					log.info("This book-chapter does not have neither ISBN10 nor ISBN13 associated with it, hence linking it to Journal.");
					pb.setVivoPubTypeDeduction("chapter-journal");
					checkJournalExistsInVivo(pb);
				}
					
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType bibo:Chapter . \n");
			}
			//Book
			else if(pb.getPubtypeStr().trim().equals("bk")) {
				pb.setVivoPubTypeDeduction("book");
				
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Book . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType bibo:Book . \n");
				if(!pb.getIsbn10().equals("null"))
					this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> bibo:isbn10 \"" + pb.getIsbn10().trim() + "\" . \n");
				if(!pb.getIsbn13().equals("null"))
					this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> bibo:isbn13 \"" + pb.getIsbn13().trim() + "\" . \n");
				if(pb.getIsbn13().equals("null") && pb.getIsbn10().equals("null") && !pb.getIssn().equals("null") && !pb.getJournal().equals("null") && !pb.getJournal().isEmpty()) {
					log.info("This book does not have neither ISBN10 nor ISBN13 associated with it, hence linking it to Journal.");
					pb.setVivoPubTypeDeduction("book-journal");
					checkJournalExistsInVivo(pb);
					
				}
					
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType bibo:Book . \n");
			}
			else if(onlyScopus){
				//this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type vivo:InformationResource . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Document . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type bibo:Article . \n");
				this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType bibo:Article . \n");
				pb.setVivoPubTypeDeduction("article");
				
				//Inference Triples
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				this.inf.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType bibo:Article . \n");
			}
			
		//}
		
		
		return pb.getVivoPubTypeDeduction();
				
	}
	
	/**
	 * @param pubType has the publication Type to be imported in VIVO 
	 * @return the free text key word to be imported in VIVO
	 */
	private String determineFreetextKeyword(String pubType) {
		String freeTextKeyword = null;
		if(pubType.equals("editorial")) {
			freeTextKeyword ="Editorial";
		}
		else if(pubType.equals("letter")) {
			freeTextKeyword = "Letter";
		}
		else if(pubType.equals("comment")) {
			freeTextKeyword = "Comment";
		}
		else if(pubType.equals("conference")) {
			freeTextKeyword = "Conference Paper";
		}
		else if(pubType.equals("review")) {
			freeTextKeyword = "Review";
		}
		else if(pubType.equals("academic-article")) {
			freeTextKeyword = "Academic Article";
		}
		else if(pubType.equals("article")) {
			freeTextKeyword = "Article";
		}
		else if(pubType.equals("press-release")) {
			freeTextKeyword = "News Release";
		}
		else if(pubType.equals("book")){
			freeTextKeyword = "Book";
		}
		else if(pubType.equals("book-journal")){
			freeTextKeyword = "Book";
		}
		else if(pubType.equals("chapter-journal")){
			freeTextKeyword = "Book Chapter";
		}
		else if(pubType.equals("book-chapter")){
			freeTextKeyword = "Book Chapter";
		}
		else if(pubType.equals("report")){
			freeTextKeyword = "Report";
		}
		else if(pubType.equals("inprocess")){
			freeTextKeyword = "In Process";
		}
		return freeTextKeyword;
	}
	
	/**
	 * @param pb PublicationBean containing all the publication information
	 * @param crudType The type of operation carried out in VIVO(UPDATE or INSERT)
	 */
	private  void updatePubAdmin(PublicationBean pb, String crudType) {
		String updateQuery = null;
		PreparedStatement ps = null;
		if(crudType.equals("INSERT")) {
			updateQuery = "update wcmc_document set vivo_ingest_date=NOW(), exists_in_vivo = 'Y' where wcmc_document_pk = " + pb.getPublicationId();

			try {
				ps = this.con.prepareStatement(updateQuery);
				log.info(ps.executeUpdate() + " row updated in pubadmin for wcmc_document_pk " + pb.getPublicationId());
			}
			catch(SQLException e) {
				log.error("SQLException" , e);
			}
			finally {
				try{
					if(ps!=null)
						ps.close();
				}
				catch(Exception e) {
					log.error("Exception",e);
				}
				
			}
		}
		else if(crudType.equals("UPDATE")) {
			updateQuery = "update wcmc_document set vivo_ingest_date_history=NOW(), exists_in_vivo = 'Y' where wcmc_document_pk = " + pb.getPublicationId();

			try {
				ps = this.con.prepareStatement(updateQuery);
				log.info(ps.executeUpdate() + " row updated in pubadmin for wcmc_document_pk " + pb.getPublicationId());
			}
			catch(SQLException e) {
				log.error("SQLException" , e);
			}
			finally {
				try{
					if(ps!=null)
						ps.close();
				}
				catch(Exception e) {
					log.error("Exception",e);
				}
				
			}
		}
		
	}
	
	
	/**
	 * This function will check for inprocess publication in VIVO and check if their publication status
	 * have changed in Pubmed or Scopus and thereby apply updates to VIVO and Pubadmin
	 * @param pb PublicationBean containing all the publication information 
	 */
	private void updateInProcessPubs(PublicationBean pb) {
		
		String urlQuery = null;
		String response = null;
		String pubType = null;
		if(pb.getPmid() != 0) {
			log.info("Searching in PUBMED for change in pubtype for pmid " + pb.getPmid());
			urlQuery = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=" +pb.getPmid() + "&retmode=xml";
			response = urlConnect(urlQuery, "PUBMED");
			if(response != null && !response.contains("<PubmedArticleSet></PubmedArticleSet>"))
				pubType = ScopusInProcessUpdate.getAndAssignPublicationType(response);
		}
		else if(pubType == null && !pb.getScopusDocId().equals("null") && !pb.getScopusDocId().isEmpty()) {// Check again in scopus if the pubtype has changed
			log.info("Searching in SCOPUS for change in pubtype for scopus-id " + pb.getScopusDocId());
			urlQuery = "https://api.elsevier.com/content/search/scopus?query=SCOPUS-ID(" + pb.getScopusDocId().trim() + ")&field=subtypeDescription";
			response = urlConnect(urlQuery, "SCOPUS");
			if(response.contains("Result set was empty")) {
				log.info("Search against scopus with SCOPUS-ID " + pb.getScopusDocId() + " is empty for in press articles.");
				if(!pb.getDoi().equals("null") && !pb.getDoi().isEmpty()) {
					log.info("Searching with DOI " + pb.getDoi() + " in SCOPUS");
					urlQuery = "https://api.elsevier.com/content/search/scopus?query=DOI(" + pb.getDoi().trim() + ")&field=subtypeDescription";
					response = urlConnect(urlQuery, "SCOPUS");
				}
			}
			if(response!= null && !response.isEmpty() && !response.contains("Result set was empty")) {	
				pubType = ScopusInProcessUpdate.getAndAssignPublicationTypefromScopus(response);
			}
		}
		
		if(pubType==null)
			log.info("No updates from scopus or pubmed for this in process publication - " + this.vivoNamespace + "pubid" + pb.getScopusDocId());
		else if(pubType.contains("InProcess")) {
			log.info("Publication is still in procees. No updates required in VIVO and Pubadmin");
		}
		else {
			//Updates required
			this.vivoJena = this.jcf.getConnectionfromPool("wcmcPublications");
			ScopusInProcessUpdate.updatePublicationTypesInVivo(this.vivoJena, pb, pubType, this.vivoNamespace);
			this.jcf.returnConnectionToPool(this.vivoJena, "wcmcPublications");
			
			ScopusInProcessUpdate.updatePubadmin(this.con, pb, pubType);
			
			this.inProcessPubsCount++;
			this.updateFlag = true;
		}
	}
	
	/**
	 * This function will update citation count from scopus(using scopus api) for existing publications in VIVO
	 * @param pb PublicationBean containing all the publication information 
	 */
	private void citationCountUpdate(PublicationBean pb) {
		
		String vivoCiteCount = null;
		int scopusCiteCount = -1;
		
		//StringBuffer queryBuf = new StringBuffer();
		if(this.sbUpdate.length() > 0)
			this.sbUpdate.setLength(0);
		
		this.sbUpdate.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
		this.sbUpdate.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
		this.sbUpdate.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
		this.sbUpdate.append("PREFIX bibo: <http://purl.org/ontology/bibo/>\n");
		this.sbUpdate.append("PREFIX c4o: <http://purl.org/spar/c4o/>\n");
		this.sbUpdate.append("PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>");
		this.sbUpdate.append("SELECT ?citeCount\n");
		this.sbUpdate.append("FROM <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications>");
		this.sbUpdate.append("WHERE {\n");
		this.sbUpdate.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId() + "> rdf:type bibo:Document .\n");
		this.sbUpdate.append("OPTIONAL {\n");
		this.sbUpdate.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId() + "> c4o:hasGlobalCitationFrequency ?frequency .\n");
		this.sbUpdate.append("?frequency rdfs:label ?citeCount .\n");
		this.sbUpdate.append("}\n");
		this.sbUpdate.append("}\n");
		
		this.vivoJena = this.jcf.getConnectionfromPool("wcmcPublications");
		ResultSet rs;
		try {
			rs = this.vivoJena.executeSelectQuery(this.sbUpdate.toString());
		while(rs.hasNext())
		{
			QuerySolution qs =rs.nextSolution();
			if(qs.get("citeCount") != null)
				vivoCiteCount = qs.get("citeCount").toString();
		}
		} catch(IOException e) {
			// TODO Auto-generated catch block
			log.info("IOException" , e);
		}
			
		
		
		//Check in Scopus if citeCount has changed or not
		String urlQuery = "https://api.elsevier.com/content/search/scopus?query=SCOPUS-ID(" + pb.getScopusDocId().trim() + ")&field=citedby-count";
		String response = urlConnect(urlQuery, "SCOPUS");
		Document doc = null;
		try {
			doc = loadXMLFromString(response);
			if(doc != null) {
				if (doc.getElementsByTagName("citedby-count").item(0) != null && doc.getElementsByTagName("citedby-count").getLength() != 0) {
					scopusCiteCount = Integer.parseInt(doc.getElementsByTagName("citedby-count").item(0).getTextContent().trim());
				}
			}
		}
		catch (Exception e) {
			log.error("extractScopusDataFromXml Exception: ", e);
		}
		
		
		if(vivoCiteCount != null && scopusCiteCount > Integer.parseInt(vivoCiteCount.trim())) {
			log.info("Citation Count in VIVO : " + vivoCiteCount + " for pubid" + pb.getScopusDocId() + " does not match with count from scopus: " + scopusCiteCount + " - Updating");
			//Update VIVO
			if(this.sbUpdate.length() > 0)
				this.sbUpdate.setLength(0);
			this.sbUpdate.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
			this.sbUpdate.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
			this.sbUpdate.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
			this.sbUpdate.append("DELETE { \n");
			this.sbUpdate.append("<" + this.vivoNamespace + "citation-pubid" + pb.getScopusDocId().trim() + "> rdfs:label \"" + vivoCiteCount + "\" . \n");
			this.sbUpdate.append("} \n");
			this.sbUpdate.append("INSERT { \n");
			this.sbUpdate.append("<" + this.vivoNamespace + "citation-pubid" + pb.getScopusDocId().trim() + "> rdfs:label \"" + scopusCiteCount + "\" . \n");
			this.sbUpdate.append("} \n");
			this.sbUpdate.append("WHERE { \n");
			this.sbUpdate.append("<" + this.vivoNamespace + "citation-pubid" + pb.getScopusDocId().trim() + "> rdfs:label \"" + vivoCiteCount + "\" . \n");
			this.sbUpdate.append("}");
			
			try {
				this.vivoJena.executeUpdateQuery(this.sbUpdate.toString(), true);
			} catch(IOException e) {
				log.error("Error connecting to SDBJena");
			}
			
			//Update Pubadmin with current citationCount
			String updateQuery = "update wcmc_document set citation_count = " + scopusCiteCount + ", citation_count_date=NOW() where wcmc_document_pk = " + pb.getPublicationId();

			PreparedStatement ps = null;

			try {
				ps = this.con.prepareStatement(updateQuery);
				log.info(ps.executeUpdate() + " row updated in pubadmin for wcmc_document_pk" + pb.getPublicationId());
			}
			catch(SQLException e) {
				log.error("SQLException" , e);
			}
			finally {
				try{
					if(ps!=null)
						ps.close();
				}
				catch(Exception e) {
					log.error("Exception",e);
				}
				
			}
			this.updateFlag = true;
			this.citeCountPubs++;
		}
		else if(vivoCiteCount != null && scopusCiteCount == Integer.parseInt(vivoCiteCount.trim())) {
			log.info("Citation Count in VIVO : " + vivoCiteCount + " for pubid" + pb.getScopusDocId() + " matches with count from scopus: " + scopusCiteCount + " - No change");
		}
		else if(vivoCiteCount == null && scopusCiteCount > 0) {
			//Insert the new citation
			if(this.sbUpdate.length() > 0)
				this.sbUpdate.setLength(0);
			this.sbUpdate.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
			this.sbUpdate.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
			this.sbUpdate.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> { \n");
			this.sbUpdate.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> <http://purl.org/spar/c4o/hasGlobalCitationFrequency> <" + this.vivoNamespace + "citation-pubid" + pb.getScopusDocId().trim() + ">  . \n");
		    this.sbUpdate.append("<" + this.vivoNamespace + "citation-pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://purl.org/spar/c4o/GlobalCitationCount> . \n");
		    this.sbUpdate.append("<" + this.vivoNamespace + "citation-pubid" + pb.getScopusDocId().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
		    this.sbUpdate.append("<" + this.vivoNamespace + "citation-pubid" + pb.getScopusDocId().trim() + "> rdfs:label \"" + scopusCiteCount + "\" . \n");
			this.sbUpdate.append("}} \n");
			
			try {
				this.vivoJena.executeUpdateQuery(this.sbUpdate.toString(), true);
			} catch(IOException e) {
				log.error("Error connecting to SDBJena");
			}
			
			//Update Pubadmin with current citationCount
			String updateQuery = "update wcmc_document set citation_count = " + scopusCiteCount + ", citation_count_date=NOW() where wcmc_document_pk = " + pb.getPublicationId();

			PreparedStatement ps = null;

			try {
				ps = this.con.prepareStatement(updateQuery);
				log.info(ps.executeUpdate() + " row updated in pubadmin for wcmc_document_pk" + pb.getPublicationId());
			}
			catch(SQLException e) {
				log.error("SQLException" , e);
			}
			finally {
				try{
					if(ps!=null)
						ps.close();
				}
				catch(Exception e) {
					log.error("Exception",e);
				}
				
			}
			this.updateFlag = true;
			this.citeCountPubs++;
			log.info("Adding new citation count for pubid " + pb.getScopusDocId() + " with count " + scopusCiteCount);
		}
		else if(vivoCiteCount == null)
			log.info("Publication does not have a citation");
		
		this.jcf.returnConnectionToPool(this.vivoJena, "wcmcPublications");
		
	}
	
	/**
	 * This function will sync current authorship in VIVO for a publication with authorships assigned in PubAdmin
	 * @param pb PublicationBean containing all the publication information 
	 */
	private void syncAuthorship(PublicationBean pb) {
		log.info("Syncing authorship with pubadmin");
		//PublicationAuthorshipMaintenance pam = new PublicationAuthorshipMaintenance();
		
		if(this.sbUpdate.length() > 0)
			this.sbUpdate.setLength(0);
		
		if(this.vivoAuthorships != null && !this.vivoAuthorships.isEmpty())
			this.vivoAuthorships.clear();
		
		//List<PublicationAuthorshipMaintenanceBean> vivoAuthorships = PublicationAuthorshipMaintenance.getAuthorshipsFromVivo(this.vivoNamespace + "pubid" + pb.getScopusDocId().trim(), this.jcf, this.sbUpdate);
		PublicationAuthorshipMaintenance.getAuthorshipsFromVivo(this.vivoNamespace + "pubid" + pb.getScopusDocId().trim(), this.vivoAuthorships, this.jcf, this.sbUpdate, this.vivoNamespace);
		log.info("List of authorship uri for publication - " + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + " in VIVO");
		for(PublicationAuthorshipMaintenanceBean pab: this.vivoAuthorships){
			log.info(pab.getAuthorshipUrl());
		}
		
		Set<AuthorBean> pubadminAuthorships = pb.getAuthorList();
		
		for(AuthorBean ab: pubadminAuthorships) {
			if(ab.getCwid() != null && !ab.getCwid().equals("null") && !ab.getCwid().isEmpty()) {
					try{
					PublicationAuthorshipMaintenanceBean p = this.vivoAuthorships.stream().filter(av -> av.getAuthorshipUrl().equals(this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "authorship" + ab.getAuthorshipRank())).findFirst().get();
					
					if(p.getAuthorUrl().contains(ab.getCwid())) {
						log.info("VIVO Authorship for publication - " + p.getPubUrl() + ": " + p.getAuthorUrl().replace(this.vivoNamespace + "cwid-", "") + " match with author from pubadmin: " + ab.getCwid());
					}
					else {
						if(this.activePeople.contains(ab.getCwid())) {
							log.info("VIVO Authorship for publication - " + p.getPubUrl() + ": " + p.getAuthorUrl().replace(this.vivoNamespace, "") + " does not match with author from pubadmin: " + ab.getCwid());
							if(this.sbUpdate.length() > 0)
								this.sbUpdate.setLength(0);
							PublicationAuthorshipMaintenance.fixAuthorshipInVivo(ab.getCwid(), p, ab, this.jcf, this.sbUpdate, this.vivoNamespace);
							this.updateFlag = true;
						}
						else {
							log.info("VIVO Authorship for publication - " + p.getPubUrl() + ": " + p.getAuthorUrl().replace(this.vivoNamespace, "") + " does not match with author from pubadmin: " + ab.getCwid() + " but the author is not an active faculty therefore is assigned as an external author");
							log.info("Authorship/s are in perfect sync. No change required.");
						}
					}
					
				}
				catch(NoSuchElementException nse) {
					log.error("The authorship url is not found in VIVO");
				}
			}
		}
	}
	
	/**
	 * This function query against scopus to find out the citation count for a publication
	 * @param queryStr The query string to connect to scopus
	 * @param source The source for the api PUBMED or SCOPUS
	 * @return the response from scopus
	 */
	private String urlConnect(String queryStr, String source) {
		StringBuffer respBuf = new StringBuffer();
		try {
			URL url = new URL(queryStr);
			URLConnection conn = url.openConnection();
			if(source.equals("SCOPUS")) {
				if(scopusApiKey != null && !scopusApiKey.trim().isEmpty())
					conn.setRequestProperty("X-ELS-APIKey", scopusApiKey.trim());
				if(scopusAccept != null && !scopusAccept.trim().isEmpty())
					conn.setRequestProperty("Accept", scopusAccept.trim());
				if(scopusInsttoken != null && !scopusInsttoken.trim().isEmpty())
					conn.setRequestProperty("X-ELS-Insttoken", scopusInsttoken.trim());
			}
			BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String inputLine;
	
			while ((inputLine = in.readLine()) != null) {
				respBuf.append(inputLine);
			}
			in.close();
		} catch (MalformedURLException e) {
			log.error("urlConnect MalformedURLException: ", e);
		} 
		catch (IOException e) {
			log.error("urlConnect IOException: ", e);
		} 
		catch (Exception e) {
			log.error("urlConnect Exception: ", e);
		}
		return respBuf.toString();
	}
	
	/**
	 * @param pb PublicationBean containing all the publication information 
	 * @return boolean whether publication exist(true) or not (false)
	 */
	private boolean checkPublicationExist(PublicationBean pb) {
		
		boolean inVivo = false;
		//StringBuilder sbs = new StringBuilder();
		if(this.sbUpdate.length() > 0)
			this.sbUpdate.setLength(0);
		
		this.sbUpdate.append("SELECT (count(?o) as ?count) ?o \n");
		this.sbUpdate.append("from <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
		this.sbUpdate.append("WHERE {\n");
		this.sbUpdate.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#mostSpecificType> ?o .");
		this.sbUpdate.append("}");
		this.sbUpdate.append("GROUP BY ?o");
		
		this.vivoJena = this.jcf.getConnectionfromPool("wcmcPublications");

		ResultSet rs;
		try {
			rs = this.vivoJena.executeSelectQuery(this.sbUpdate.toString());
			QuerySolution qs = rs.nextSolution();
			int count = Integer.parseInt(qs.get("count").toString().replace("^^http://www.w3.org/2001/XMLSchema#integer", ""));
			if(qs.get("o") != null) {
			String pubType = qs.get("o").toString();
				if(pubType != null && !pubType.isEmpty())
					pb.setVivoPubTypeDeduction(pubType);
			}
			if(count == 0) 
				inVivo = false;
			else
				inVivo = true;
			} catch(IOException e) {
				// TODO Auto-generated catch block
				log.info("IOException" , e);
			}
		
		this.jcf.returnConnectionToPool(this.vivoJena, "wcmcPublications");
		
		return inVivo;
	}
	
	/**
	 * @param ab Author containing all the authorship information for a publication
	 * @return boolean whether author exist(true) or not (false)
	 */
	private boolean checkExternalAuthorExists(AuthorBean ab) {

		boolean inVivo = false;
		
		//Check for person in VIVO
		//StringBuilder sbs = new StringBuilder();
		if(this.sbUpdate.length() > 0)
			this.sbUpdate.setLength(0);
		
		
		this.sbUpdate.append("SELECT (count(?o) as ?count) \n");
		this.sbUpdate.append("from <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
		this.sbUpdate.append("WHERE {\n");
		this.sbUpdate.append("<" + this.vivoNamespace + "person" + ab.getAuthorshipPk() + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o .");
		this.sbUpdate.append("}");
		
		//log.info(sbs.toString());
		this.vivoJena = this.jcf.getConnectionfromPool("wcmcPublications");
		
		ResultSet rs;
		try {
			rs = this.vivoJena.executeSelectQuery(this.sbUpdate.toString());
			int count = Integer.parseInt(rs.nextSolution().get("count").toString().replace("^^http://www.w3.org/2001/XMLSchema#integer", ""));
			if(count == 0) 
				inVivo = false;
			else
				inVivo = true;
			} catch(IOException e) {
				// TODO Auto-generated catch block
				log.info("IOException" , e);
			}
		
		this.jcf.returnConnectionToPool(this.vivoJena, "wcmcPublications");
		
		return inVivo;
	}
	
	/**
	 * Function to check whether a meshMajor already exist in VIVO
	 * @param meshMajor meshMajor String joined with a `|`
	 * @return boolean value which will state if meshMajr exists in VIVO or not
	 */
	private boolean checkMeshMajorExists(String meshMajor) {
		boolean inVivo = false;
		
		if(this.sbUpdate.length() > 0)
			this.sbUpdate.setLength(0);
		
		this.sbUpdate.append("SELECT (count(?o) as ?count) \n");
		this.sbUpdate.append("from <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
		this.sbUpdate.append("WHERE { \n");
		this.sbUpdate.append("<" + this.vivoNamespace + "concept" + meshMajor + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o . \n");
		this.sbUpdate.append("}");
		
		
		this.vivoJena = this.jcf.getConnectionfromPool("wcmcPublications");
		ResultSet rs;
		
		try {
			rs = this.vivoJena.executeSelectQuery(this.sbUpdate.toString());
			int count = Integer.parseInt(rs.nextSolution().get("count").toString().replace("^^http://www.w3.org/2001/XMLSchema#integer", ""));
			if(count == 0) 
				inVivo = false;
			else
				inVivo = true;
		
		} catch(IOException e) {
			log.error("Error connecting to Jena Database" , e);
		}
		this.jcf.returnConnectionToPool(this.vivoJena, "wcmcPublications");
		
		return inVivo;
	}
	
	/**
	 * This function check whether a Journal or a Book exists in VIVO and then return boolean value. Also if its a journal and if it does not exist then it
	 * returns appended triples for the insertQuery buffer for a new journal in VIVO
	 * @param pb PublicationBean having all the publication details
	 * @return boolean value which will state if journal exists or not
	 */
	private boolean checkJournalExistsInVivo(PublicationBean pb) {
		boolean inVivo = false;
		String journalUri = null;
		String bookUri = null;
		String lissn = null;
		
		//if(this.journalIdMap != null && !this.journalIdMap.isEmpty())
			this.journalIdMap.clear();
		
		//Check if its a book or not
		if(!pb.getVivoPubTypeDeduction().equals("book-chapter") && !pb.getVivoPubTypeDeduction().equals("book")) {
			//Different conditions for searching for journal in VIVO
				if(!pb.getIssn().equals("null") && !pb.getIssn().isEmpty()) { 
					journalUri = this.vivoNamespace + "journal" + pb.getIssn().trim();
					
				}
				else if(pb.getIssn().equals("null") && !pb.getEissn().equals("null") && !pb.getEissn().isEmpty()) {
					journalUri = this.vivoNamespace + "journal" + pb.getEissn().trim();
				}
				else if(pb.getIssn().equals("null") && pb.getEissn().equals("null") && !pb.getPubmedXmlContent().equals("null")  && pb.getPubmedXmlContent().contains("ISSNLinking")) {
					//This is the case to look in pubmedXMlContent if it exists for the linking issn
					Document doc = null;
					try {
						doc = loadXMLFromString(pb.getPubmedXmlContent());
						if(doc != null) {
							if(doc.getElementsByTagName("ISSNLinking")!=null && doc.getElementsByTagName("ISSNLinking").getLength() != 0) {
								lissn = doc.getElementsByTagName("ISSNLinking").item(0).getTextContent();
								journalUri = this.vivoNamespace + "journal" + lissn;
							}
						}
					}
					catch (Exception e) {
						log.error("extractPubmedDataFromXml Exception: ", e);
					}
				}
				else  {
					//If all else fails then go with hash of journal name
					journalUri = this.vivoNamespace + "journal" + keyHash(pb.getJournal());
				}
				
				if(!pb.getIssn().equals("null") && !pb.getIssn().isEmpty())
					this.journalIdMap.put("ISSN", pb.getIssn());
				
				if(!pb.getEissn().equals("null") && !pb.getEissn().isEmpty())
					this.journalIdMap.put("EISSN", pb.getEissn());
				
				if(!pb.getPubmedXmlContent().equals("null") && pb.getPubmedXmlContent().contains("ISSNLinking")) {
					Document doc = null;
					try {
						doc = loadXMLFromString(pb.getPubmedXmlContent());
						if(doc != null) {
							if(doc.getElementsByTagName("ISSNLinking")!=null && doc.getElementsByTagName("ISSNLinking").getLength() != 0) {
								lissn = doc.getElementsByTagName("ISSNLinking").item(0).getTextContent();
								this.journalIdMap.put("LISSN", lissn);
							}
						}
					}
					catch (Exception e) {
						log.error("extractPubmedDataFromXml Exception: ", e);
					}
				}
				
		}
		
		
			
		if(this.sbUpdate.length() > 0)
			this.sbUpdate.setLength(0);

		this.sbUpdate.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
		this.sbUpdate.append("PREFIX bibo: <http://purl.org/ontology/bibo/> \n");
		this.sbUpdate.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
		if((pb.getVivoPubTypeDeduction().equals("book") || pb.getVivoPubTypeDeduction().equals("book-chapter")))
			this.sbUpdate.append("SELECT ?book \n");
		else
			this.sbUpdate.append("SELECT ?journal \n");
		this.sbUpdate.append("from <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
		this.sbUpdate.append("WHERE { \n");
		if((pb.getVivoPubTypeDeduction().equals("book") || pb.getVivoPubTypeDeduction().equals("book-chapter"))) {

			if(!pb.getIsbn10().equals("null"))
				this.sbUpdate.append("OPTIONAL {?book bibo:isbn10 \"" + pb.getIsbn10() + "\" . }\n");
			if(!pb.getIsbn13().equals("null"))
				this.sbUpdate.append("OPTIONAL {?book bibo:isbn13 \"" + pb.getIsbn13() + "\" . }\n");
		}
		else {
			if(!this.journalIdMap.isEmpty()) {
				for (Entry<String, String> entry : this.journalIdMap.entrySet()) {
				    if(entry.getKey().equals("ISSN"))
				    	this.sbUpdate.append("OPTIONAL {?journal bibo:issn \"" + entry.getValue() + "\" . }\n");
				    if(entry.getKey().equals("EISSN"))
				    	this.sbUpdate.append("OPTIONAL {?journal bibo:eissn \"" + entry.getValue() + "\" . }\n");
				    if(entry.getKey().equals("LISSN"))
				    	this.sbUpdate.append("OPTIONAL {?journal wcmc:lissn \"" + entry.getValue() + "\" . }\n");
				    
				}
			}
		}
		this.sbUpdate.append("}");

		this.vivoJena = this.jcf.getConnectionfromPool("wcmcPublications");
		ResultSet rs;
		int count = 0;
		try {
			rs = this.vivoJena.executeSelectQuery(this.sbUpdate.toString());
			if((pb.getVivoPubTypeDeduction().equals("book") || pb.getVivoPubTypeDeduction().equals("book-chapter")) ) {

				if(rs.hasNext()) {
					QuerySolution qs = rs.nextSolution();
					if(qs.get("book")!= null) {
						bookUri = qs.get("book").toString();
						count = 1;
					}
				}
			}
			else {
				if(rs.hasNext()) {
					QuerySolution qs = rs.nextSolution();
					if(qs.get("journal")!= null) {
						journalUri = qs.get("journal").toString();
						count = 1;
					}
				}
			}
			
			if(count == 0) 
				inVivo = false;
			else
				inVivo = true;
		
		} catch(IOException e) {
			log.error("Error connecting to Jena Database" , e);
		}

		this.jcf.returnConnectionToPool(this.vivoJena, "wcmcPublications");
		
		//Case when journal does not exist in VIVO
		if((!pb.getVivoPubTypeDeduction().equals("book") && !pb.getVivoPubTypeDeduction().equals("book-chapter"))  && journalUri != null && !inVivo) {
			log.info("Journal - " + journalUri + "(" + pb.getJournal() + ") does not exist in VIVO. Creating it.");
			this.sbInsert.append("<" + journalUri + "> rdf:type obo:BFO_0000001 . \n");
			this.sbInsert.append("<" + journalUri + "> rdf:type obo:BFO_0000002 . \n");
			this.sbInsert.append("<" + journalUri + "> rdf:type obo:BFO_0000031 . \n");
			this.sbInsert.append("<" + journalUri + "> rdf:type obo:IAO_0000030 . \n");
			this.sbInsert.append("<" + journalUri + "> rdf:type bibo:Collection . \n");
			this.sbInsert.append("<" + journalUri + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
			this.sbInsert.append("<" + journalUri + "> rdf:type bibo:Periodical . \n");
			this.sbInsert.append("<" + journalUri + "> rdf:type bibo:Journal . \n");
			this.sbInsert.append("<" + journalUri + "> vitro:mostSpecificType bibo:Journal . \n");
			this.sbInsert.append("<" + journalUri + "> core:Title \"" + pb.getJournal().trim() + "\" . \n");
			this.sbInsert.append("<" + journalUri + "> rdfs:label \"" + pb.getJournal().trim() + "\" . \n");
			if(!pb.getNlmabbreviation().equals("null") && !pb.getNlmabbreviation().isEmpty())
				this.sbInsert.append("<" + journalUri + "> wcmc:ISOAbbreviation \"" + pb.getNlmabbreviation().trim() + "\" . \n");
			this.sbInsert.append("<" + journalUri + "> <http://vivoweb.org/ontology/core#publicationVenueFor> <" + this.vivoNamespace + "pubid" + pb.getScopusDocId() + "> . \n");
			
			if(!pb.getIssn().equals("null") && !pb.getIssn().isEmpty())
				this.sbInsert.append("<" + journalUri + "> bibo:issn \"" + pb.getIssn().trim() + "\" . \n");
			if(!pb.getEissn().equals("null") && !pb.getEissn().isEmpty())
				this.sbInsert.append("<" + journalUri + "> bibo:eissn \"" + pb.getEissn().trim() + "\" . \n");
			if(!pb.getPubmedXmlContent().equals("null") && pb.getPubmedXmlContent().contains("ISSNLinking")) {
				try {
					Document doc = loadXMLFromString(pb.getPubmedXmlContent());
					if(doc != null) {
						if(doc.getElementsByTagName("ISSNLinking")!=null && doc.getElementsByTagName("ISSNLinking").getLength() != 0) {
							lissn = doc.getElementsByTagName("ISSNLinking").item(0).getTextContent();
							this.sbInsert.append("<" + journalUri + "> wcmc:lissn \"" + lissn.trim() + "\" . \n");
						}
					}
				}
				catch (Exception e) {
					log.error("extractPubmedDataFromXml Exception: ", e);
				}
				
			}
			this.sbInsert.append("<" + journalUri + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"Scopus-Pubmed-Harvester\" . \n");
			this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId() + "> <http://vivoweb.org/ontology/core#hasPublicationVenue> <" + journalUri + "> . \n");
			
		}
		//This is when journal or book exists in VIVO
		else if(journalUri != null && inVivo && (!pb.getVivoPubTypeDeduction().equals("book") && !pb.getVivoPubTypeDeduction().equals("book-chapter"))) {
			log.info("Journal - " + journalUri + "(" + pb.getJournal() + ") already exist in VIVO. Linking publication " + pb.getScopusDocId().trim() + " to it");
			this.sbInsert.append("<" + journalUri + "> <http://vivoweb.org/ontology/core#publicationVenueFor> <" + this.vivoNamespace + "pubid" + pb.getScopusDocId() + "> . \n");
			this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId() + "> <http://vivoweb.org/ontology/core#hasPublicationVenue> <" + journalUri + "> . \n");
			
			//Check to see if it has lissn , issn and eissn. If not then add it to the journal
			
			if(this.sbUpdate.length() > 0)
				this.sbUpdate.setLength(0);

			this.sbUpdate.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
			this.sbUpdate.append("PREFIX bibo: <http://purl.org/ontology/bibo/> \n");
			this.sbUpdate.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
			this.sbUpdate.append("SELECT ?issn ?eissn ?lissn \n");
			this.sbUpdate.append("from <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
			this.sbUpdate.append("WHERE { \n");
			this.sbUpdate.append("OPTIONAL {<" + journalUri + "> bibo:issn ?issn . }\n");
			this.sbUpdate.append("OPTIONAL {<" + journalUri + "> bibo:eissn ?eissn . }\n");
			this.sbUpdate.append("OPTIONAL {<" + journalUri + "> wcmc:lissn ?lissn . }\n");
			this.sbUpdate.append("} \n");
			
			//log.info(this.sbUpdate.toString());
			this.vivoJena = this.jcf.getConnectionfromPool("wcmcPublications");
			try {
				rs = this.vivoJena.executeSelectQuery(this.sbUpdate.toString());
				
				if(rs.hasNext()) {
					QuerySolution qs = rs.nextSolution();
					if(!this.journalIdMap.isEmpty()) {
						for (Entry<String, String> entry : this.journalIdMap.entrySet()) {
							if(qs.get("issn") == null) {
								if(entry.getKey().equals("ISSN")) {
									this.sbInsert.append("<" + journalUri + "> bibo:issn \"" + entry.getValue().trim() + "\" . \n");
									log.info("Adding ISSN: " + entry.getValue() + " to the journal " + journalUri);
								}
							}
							if(qs.get("eissn") == null) {
								if(entry.getKey().equals("EISSN")) {
									this.sbInsert.append("<" + journalUri + "> bibo:eissn \"" + entry.getValue().trim() + "\" . \n");
									log.info("Adding EISSN: " + entry.getValue() + " to the journal " + journalUri);
								}
							}
							if(qs.get("lissn") == null) {
								if(entry.getKey().equals("LISSN")) {
									this.sbInsert.append("<" + journalUri + "> wcmc:lissn \"" + entry.getValue().trim() + "\" . \n");
									log.info("Adding LISSN: " + entry.getValue() + " to the journal " + journalUri);
								}
							}
						}
					}
				}
			} catch(IOException e) {
				log.error("Error connecting to Jena Database" , e);
			}
			this.jcf.returnConnectionToPool(this.vivoJena, "wcmcPublications");
			
			
			
		}
		else if(!inVivo && pb.getVivoPubTypeDeduction().equals("book-chapter")) {
			log.info("Book - " + pb.getJournal() + " does not exist in VIVO."); //Have to create the book object first and then link chapter to it
			if(pb.getIsbn13().equals("null") && pb.getIsbn10().equals("null") && !pb.getIssn().equals("null") && !pb.getJournal().equals("null") && !pb.getJournal().isEmpty())
				log.info("This book chapter is is associated with Journal " + pb.getJournal() + " instead of a book. Linking to it");
		}
		else if(inVivo && bookUri != null && (pb.getVivoPubTypeDeduction().equals("book-chapter"))) {
			log.info("Book - " + bookUri + "(" + pb.getJournal() + ") already exist in VIVO. Linking publication(chapter) - pubid" + pb.getScopusDocId().trim() + " to it");
			this.sbInsert.append("<" + bookUri + "> obo:BFO_0000051 <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> . \n");
			this.sbInsert.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> obo:BFO_0000050 <" + bookUri + "> . \n");
		}
		
		return inVivo;
	}
	
	/**
	 * This function will link chapters to book as "part of other document" object if for some reason chapter was created before the book was created. This will ensure that all the chapter object
	 * link with its respective book object if it exist in VIVO
	 * @param pb PublicationBean having all the publication details
	 */
	private void linkBookToChapters(PublicationBean pb) {
		
		String bookUri = null;
		boolean bookChapterLink = false;
		
		if(this.sbUpdate.length()>0)
			this.sbUpdate.setLength(0);
		
		this.sbUpdate.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
		this.sbUpdate.append("PREFIX obo: <http://purl.obolibrary.org/obo/>");
		this.sbUpdate.append("PREFIX bibo: <http://purl.org/ontology/bibo/> \n");
		this.sbUpdate.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
		this.sbUpdate.append("SELECT ?book ?bookPart \n");
		this.sbUpdate.append("from <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
		this.sbUpdate.append("WHERE { \n");
		if(!pb.getIsbn10().equals("null"))
			this.sbUpdate.append("OPTIONAL {?book bibo:isbn10 \"" + pb.getIsbn10() + "\" . }\n");
		if(!pb.getIsbn13().equals("null"))
			this.sbUpdate.append("OPTIONAL {?book bibo:isbn13 \"" + pb.getIsbn13() + "\" . }\n");
		this.sbUpdate.append("OPTIONAL {<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> obo:BFO_0000050 ?bookPart . }\n");
		this.sbUpdate.append("}");
		
		this.vivoJena = this.jcf.getConnectionfromPool("wcmcPublications");
		ResultSet rs;
		try {
			rs = this.vivoJena.executeSelectQuery(this.sbUpdate.toString());

			if(rs.hasNext()) {
				QuerySolution qs = rs.nextSolution();
				if(qs.get("book")!= null) {
					bookUri = qs.get("book").toString();
				}
				if(qs.get("bookPart")!= null) {
					bookChapterLink = true;
				}
			}
		
		} catch(IOException e) {
			log.error("Error connecting to Jena Database" , e);
		}

		if(bookUri != null && !bookChapterLink) {
			log.info("Book - " + pb.getJournal() + " exist in VIVO but not linked to Chapter - pubid" + pb.getScopusDocId() + ". Linking it.");
			
			if(this.sbUpdate.length()>0)
				this.sbUpdate.setLength(0);
			this.sbUpdate.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
			this.sbUpdate.append("PREFIX obo: <http://purl.obolibrary.org/obo/>");
			this.sbUpdate.append("PREFIX bibo: <http://purl.org/ontology/bibo/> \n");
			this.sbUpdate.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
			this.sbUpdate.append("INSERT DATA { \n");
			this.sbUpdate.append("GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
			this.sbUpdate.append("{ \n");
			this.sbUpdate.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> obo:BFO_0000050 <" + bookUri + "> . \n");
			this.sbUpdate.append("<" + bookUri + "> obo:BFO_0000051 <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> . \n");
			this.sbUpdate.append("}}");
			log.info(this.sbUpdate.toString());
			
			try {
				this.vivoJena.executeUpdateQuery(this.sbUpdate.toString(), true);
			} catch(IOException e) {
				log.error("Error connecting to SDBJena", e);
			}
		}
		else
			log.info("Book to Chapter link for pubid" + pb.getScopusDocId() + " looks good");
		
		this.jcf.returnConnectionToPool(this.vivoJena, "wcmcPublications");
		
	}
	
	/**
	 * This function deals syncing Journals with publication for source data. 
	 * @param pb PublicationBean having all the publication details
	 */
	private void journalSync(PublicationBean pb) {
		
		String journalUri = null;
		String lissn = null;
		String journalVivo = null;
		if(!pb.getIssn().equals("null") && !pb.getIssn().isEmpty()) { 
			journalUri = this.vivoNamespace + "journal" + pb.getIssn().trim();
			
		}
		else if(pb.getIssn().equals("null") && !pb.getEissn().equals("null") && !pb.getEissn().isEmpty()) {
			journalUri = this.vivoNamespace + "journal" + pb.getEissn().trim();
		}
		else if(pb.getIssn().equals("null") && pb.getEissn().equals("null") && !pb.getPubmedXmlContent().equals("null") && pb.getPubmedXmlContent().contains("ISSNLinking")) {
			//This is the case to look in pubmedXMlContent if it exists for the linking issn
			Document doc = null;
			try {
				doc = loadXMLFromString(pb.getPubmedXmlContent());
				if(doc != null) {
					if(doc.getElementsByTagName("ISSNLinking")!=null && doc.getElementsByTagName("ISSNLinking").getLength() != 0) {
						lissn = doc.getElementsByTagName("ISSNLinking").item(0).getTextContent();
						journalUri = this.vivoNamespace + "journal" + lissn;
					}
				}
			}
			catch (Exception e) {
				log.error("extractPubmedDataFromXml Exception: ", e);
			}
		}
		else  {
			//If all else fails then go with hash of journal name
			journalUri = this.vivoNamespace + "journal" + keyHash(pb.getJournal());
		}
		
		if(!pb.getPubmedXmlContent().equals("null") && pb.getPubmedXmlContent().contains("ISSNLinking")) {
			Document doc = null;
			try {
				doc = loadXMLFromString(pb.getPubmedXmlContent());
				if(doc != null) {
					if(doc.getElementsByTagName("ISSNLinking")!=null && doc.getElementsByTagName("ISSNLinking").getLength() != 0) {
						lissn = doc.getElementsByTagName("ISSNLinking").item(0).getTextContent();
					}
				}
			}
			catch (Exception e) {
				log.error("extractPubmedDataFromXml Exception: ", e);
			}
		}
		
		if(this.sbUpdate.length()>0)
			this.sbUpdate.setLength(0);
		
		this.sbUpdate.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
		this.sbUpdate.append("PREFIX obo: <http://purl.obolibrary.org/obo/>");
		this.sbUpdate.append("PREFIX bibo: <http://purl.org/ontology/bibo/> \n");
		this.sbUpdate.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
		this.sbUpdate.append("SELECT ?journalVivo \n");
		this.sbUpdate.append("from <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
		this.sbUpdate.append("WHERE { \n");
		this.sbUpdate.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> <http://vivoweb.org/ontology/core#hasPublicationVenue> ?journalVivo .\n");
		this.sbUpdate.append("}");
		
		this.vivoJena = this.jcf.getConnectionfromPool("wcmcPublications");
		ResultSet rs;
		try {
			rs = this.vivoJena.executeSelectQuery(this.sbUpdate.toString());

			if(rs.hasNext()) {
				QuerySolution qs = rs.nextSolution();
				if(qs.get("journalVivo")!= null) {
					journalVivo = qs.get("journalVivo").toString();
				}
			}
		
		} catch(IOException e) {
			log.error("Error connecting to Jena Database" , e);
		}
		
		if(journalUri != null && journalVivo != null && !journalUri.equals(journalVivo)) {
			log.info("Journal in VIVO: " + journalVivo + " does not match with Journal from Pubadmin: " + journalUri);
			
			//Check for the journal Uri from pubadmin exist in VIVO
			
			if(this.sbUpdate.length()>0)
				this.sbUpdate.setLength(0);
			
			this.sbUpdate.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
			this.sbUpdate.append("PREFIX obo: <http://purl.obolibrary.org/obo/>");
			this.sbUpdate.append("PREFIX bibo: <http://purl.org/ontology/bibo/> \n");
			this.sbUpdate.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
			this.sbUpdate.append("SELECT (count(?o) as ?count) \n");
			this.sbUpdate.append("from <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
			this.sbUpdate.append("WHERE { \n");
			this.sbUpdate.append("<" + journalUri + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o .\n");
			this.sbUpdate.append("}");
			
			try {
				rs = this.vivoJena.executeSelectQuery(this.sbUpdate.toString());
				int count = Integer.parseInt(rs.nextSolution().get("count").toString().replace("^^http://www.w3.org/2001/XMLSchema#integer", ""));
				if(count > 0) {
					log.info("Correct Journal from Pubadmin already exist in VIVO. Replacing the incorrect journal link " + journalVivo + " for the publication with " + journalUri);
					
					String journalCheck = null;
					boolean journalTag = false;
					
					if(this.sbUpdate.length()>0)
						this.sbUpdate.setLength(0);
					
					this.sbUpdate.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
					this.sbUpdate.append("PREFIX obo: <http://purl.obolibrary.org/obo/>");
					this.sbUpdate.append("PREFIX bibo: <http://purl.org/ontology/bibo/> \n");
					this.sbUpdate.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
					this.sbUpdate.append("SELECT ?journalUri \n");
					this.sbUpdate.append("from <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
					this.sbUpdate.append("WHERE { \n");
					
					if(!pb.getIssn().equals("null") && !pb.getIssn().isEmpty())
						this.sbUpdate.append("OPTIONAL {?journalUri bibo:issn \"" + pb.getIssn().trim() + "\" . }\n");
					if(!pb.getEissn().equals("null") && !pb.getEissn().isEmpty())
						this.sbUpdate.append("OPTIONAL {?journalUri bibo:eissn \"" + pb.getEissn().trim() + "\" . }\n");
					if(lissn!=null && !lissn.isEmpty())
						this.sbUpdate.append("OPTIONAL {?journalUri wcmc:lissn \"" + lissn.trim() + "\" . }\n");
					this.sbUpdate.append("}");
					
					rs = this.vivoJena.executeSelectQuery(this.sbUpdate.toString());

					while(rs.hasNext()) {
						QuerySolution qs = rs.nextSolution();
						if(qs.get("journalUri")!= null) {
							journalCheck = qs.get("journalUri").toString();
							if(journalCheck.equals(journalVivo))
								journalTag = true;
						}
					}
					if(journalTag) {
						log.info("Journal is correctly tagged");
					}
					else {
					
						if(this.sbUpdate.length() > 0)
							this.sbUpdate.setLength(0);
						
						this.sbUpdate.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
						this.sbUpdate.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
						this.sbUpdate.append("DELETE { \n");
						this.sbUpdate.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> <http://vivoweb.org/ontology/core#hasPublicationVenue> <" + journalVivo + "> . \n");
						this.sbUpdate.append("<" + journalVivo + "> <http://vivoweb.org/ontology/core#publicationVenueFor> <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> . \n");
						this.sbUpdate.append("} \n");
						this.sbUpdate.append("INSERT { \n");
						this.sbUpdate.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> <http://vivoweb.org/ontology/core#hasPublicationVenue> <" + journalUri + "> . \n");
						this.sbUpdate.append("<" + journalUri + "> <http://vivoweb.org/ontology/core#publicationVenueFor> <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> . \n");
						this.sbUpdate.append("} \n");
						this.sbUpdate.append("WHERE { \n");
						this.sbUpdate.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> <http://vivoweb.org/ontology/core#hasPublicationVenue> <" + journalVivo + "> . \n");
						this.sbUpdate.append("<" + journalVivo + "> <http://vivoweb.org/ontology/core#publicationVenueFor> <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> . \n");
						this.sbUpdate.append("}");
						
						log.info(this.sbUpdate.toString());
						
						try {
							this.vivoJena.executeUpdateQuery(this.sbUpdate.toString(), true);
						} catch(IOException e) {
							log.error("Error connecting to SDBJena");
						}
						this.updateFlag = true;
					}
				}
				else {
					log.info("Checking Journal object: " + journalUri + " in VIVO and and its corresponding publication: pubid" + pb.getScopusDocId());
					
					String journalCheck = null;
					
					if(this.sbUpdate.length()>0)
						this.sbUpdate.setLength(0);
					
					this.sbUpdate.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
					this.sbUpdate.append("PREFIX obo: <http://purl.obolibrary.org/obo/>");
					this.sbUpdate.append("PREFIX bibo: <http://purl.org/ontology/bibo/> \n");
					this.sbUpdate.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
					this.sbUpdate.append("SELECT ?journalUri \n");
					this.sbUpdate.append("from <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
					this.sbUpdate.append("WHERE { \n");
					
					if(!pb.getIssn().equals("null") && !pb.getIssn().isEmpty())
						this.sbUpdate.append("OPTIONAL {?journalUri bibo:issn \"" + pb.getIssn().trim() + "\" . }\n");
					if(!pb.getEissn().equals("null") && !pb.getEissn().isEmpty())
						this.sbUpdate.append("OPTIONAL {?journalUri bibo:eissn \"" + pb.getEissn().trim() + "\" . }\n");
					if(lissn!=null && !lissn.isEmpty())
						this.sbUpdate.append("OPTIONAL {?journalUri wcmc:lissn \"" + lissn.trim() + "\" . }\n");
					this.sbUpdate.append("}");
					
					rs = this.vivoJena.executeSelectQuery(this.sbUpdate.toString());

					if(rs.hasNext()) {
						QuerySolution qs = rs.nextSolution();
						if(qs.get("journalUri")!= null) {
							journalCheck = qs.get("journalUri").toString();
						}
					}
					
					if(journalCheck != null) {
						
						log.info("Journal " + journalCheck + " exist in VIVO based on issn and eissn lookup. Linking to it");
						if(!journalCheck.equals(journalVivo)) {
							if(this.sbUpdate.length() > 0)
								this.sbUpdate.setLength(0);
							
							this.sbUpdate.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
							this.sbUpdate.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
							this.sbUpdate.append("DELETE { \n");
							this.sbUpdate.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> <http://vivoweb.org/ontology/core#hasPublicationVenue> <" + journalVivo + "> . \n");
							this.sbUpdate.append("<" + journalVivo + "> <http://vivoweb.org/ontology/core#publicationVenueFor> <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> . \n");
							this.sbUpdate.append("} \n");
							this.sbUpdate.append("INSERT { \n");
							this.sbUpdate.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> <http://vivoweb.org/ontology/core#hasPublicationVenue> <" + journalCheck + "> . \n");
							this.sbUpdate.append("<" + journalCheck + "> <http://vivoweb.org/ontology/core#publicationVenueFor> <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> . \n");
							this.sbUpdate.append("} \n");
							this.sbUpdate.append("WHERE { \n");
							this.sbUpdate.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> <http://vivoweb.org/ontology/core#hasPublicationVenue> <" + journalVivo + "> . \n");
							this.sbUpdate.append("<" + journalVivo + "> <http://vivoweb.org/ontology/core#publicationVenueFor> <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> . \n");
							this.sbUpdate.append("}");
							
							log.info(this.sbUpdate.toString());
							
							try {
								this.vivoJena.executeUpdateQuery(this.sbUpdate.toString(), true);
							} catch(IOException e) {
								log.error("Error connecting to SDBJena");
							}
							this.updateFlag = true;
						}
						else
							log.info("Journal Tagging is correct");
						
					}
					else {
					
						//Correct Journal does not exist in VIVO yet Create it
						if(this.sbUpdate.length() > 0)
							this.sbUpdate.setLength(0);
						
						this.sbUpdate.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						this.sbUpdate.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
						this.sbUpdate.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
						this.sbUpdate.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
						this.sbUpdate.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
						this.sbUpdate.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
						this.sbUpdate.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
						this.sbUpdate.append("PREFIX bibo: <http://purl.org/ontology/bibo/> \n");
						this.sbUpdate.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
						this.sbUpdate.append("DELETE { \n");
						this.sbUpdate.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> <http://vivoweb.org/ontology/core#hasPublicationVenue> <" + journalVivo + "> . \n");
						this.sbUpdate.append("<" + journalVivo + "> <http://vivoweb.org/ontology/core#publicationVenueFor> <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> . \n");
						this.sbUpdate.append("} \n");
						this.sbUpdate.append("INSERT { \n");
						this.sbUpdate.append("<" + journalUri + "> rdf:type obo:BFO_0000001 . \n");
						this.sbUpdate.append("<" + journalUri + "> rdf:type obo:BFO_0000002 . \n");
						this.sbUpdate.append("<" + journalUri + "> rdf:type obo:BFO_0000031 . \n");
						this.sbUpdate.append("<" + journalUri + "> rdf:type obo:IAO_0000030 . \n");
						this.sbUpdate.append("<" + journalUri + "> rdf:type bibo:Collection . \n");
						this.sbUpdate.append("<" + journalUri + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
						this.sbUpdate.append("<" + journalUri + "> rdf:type bibo:Periodical . \n");
						this.sbUpdate.append("<" + journalUri + "> rdf:type bibo:Journal . \n");
						this.sbUpdate.append("<" + journalUri + "> vitro:mostSpecificType bibo:Journal . \n");
						this.sbUpdate.append("<" + journalUri + "> core:Title \"" + pb.getJournal().trim() + "\" . \n");
						this.sbUpdate.append("<" + journalUri + "> rdfs:label \"" + pb.getJournal().trim() + "\" . \n");
						if(!pb.getNlmabbreviation().equals("null") && !pb.getNlmabbreviation().isEmpty())
							this.sbUpdate.append("<" + journalUri + "> wcmc:ISOAbbreviation \"" + pb.getNlmabbreviation().trim() + "\" . \n");
						
						if(!pb.getIssn().equals("null") && !pb.getIssn().isEmpty())
							this.sbUpdate.append("<" + journalUri + "> bibo:issn \"" + pb.getIssn().trim() + "\" . \n");
						if(!pb.getEissn().equals("null") && !pb.getEissn().isEmpty())
							this.sbUpdate.append("<" + journalUri + "> bibo:eissn \"" + pb.getEissn().trim() + "\" . \n");
	
						//case if journal uri is assigned by issn or eissn then to add the lissn if it exists as triple
						if(!pb.getPubmedXmlContent().equals("null") && pb.getPubmedXmlContent().contains("ISSNLinking") ) {
							try {
								Document doc = loadXMLFromString(pb.getPubmedXmlContent());
								if(doc != null) {
									if(doc.getElementsByTagName("ISSNLinking")!=null && doc.getElementsByTagName("ISSNLinking").getLength() != 0) {
										lissn = doc.getElementsByTagName("ISSNLinking").item(0).getTextContent();
										this.sbUpdate.append("<" + journalUri + "> wcmc:lissn \"" + lissn.trim() + "\" . \n");
									}
								}
							}
							catch (Exception e) {
								log.error("extractPubmedDataFromXml Exception: ", e);
							}
							
						}
						this.sbUpdate.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> <http://vivoweb.org/ontology/core#hasPublicationVenue> <" + journalUri + "> . \n");
						this.sbUpdate.append("<" + journalUri + "> <http://vivoweb.org/ontology/core#publicationVenueFor> <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> . \n");
						this.sbUpdate.append("<" + journalUri + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"Scopus-Pubmed-Harvester\" . \n");
						this.sbUpdate.append("} \n");
						this.sbUpdate.append("WHERE { \n");
						this.sbUpdate.append("<" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> <http://vivoweb.org/ontology/core#hasPublicationVenue> <" + journalVivo + "> . \n");
						this.sbUpdate.append("<" + journalVivo + "> <http://vivoweb.org/ontology/core#publicationVenueFor> <" + this.vivoNamespace + "pubid" + pb.getScopusDocId().trim() + "> . \n");
						this.sbUpdate.append("}");
						
						log.info(this.sbUpdate.toString());
						
						try {
							this.vivoJena.executeUpdateQuery(this.sbUpdate.toString(), true);
						} catch(IOException e) {
							log.error("Error connecting to SDBJena");
						}
						this.updateFlag = true;
					}
					
					
				}
				
					
			
			} catch(IOException e) {
				log.error("Error connecting to Jena Database" , e);
			}
			
			
		}
		else
			log.info("Journal is in perfect sync with Pubadmin for pubid" + pb.getScopusDocId());
		
		this.jcf.returnConnectionToPool(this.vivoJena, "wcmcPublications");
	}
	
	public static Document loadXMLFromString(String xml) throws Exception {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setIgnoringComments(true);
		DocumentBuilder builder = factory.newDocumentBuilder();
		InputSource is = new InputSource(new StringReader(xml));
		return builder.parse(is);
	}
	
	/**
     * This function generates a hashcode for a string name (e.g. department or journal)
     * @param dept String containing the name
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
