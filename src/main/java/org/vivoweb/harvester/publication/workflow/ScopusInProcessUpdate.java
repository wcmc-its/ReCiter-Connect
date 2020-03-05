package org.vivoweb.harvester.publication.workflow;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vivoweb.harvester.ingest.PublicationFetch;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;
import org.w3c.dom.Document;

import reciter.connect.beans.vivo.PublicationBean;

/**
 * @author szd2013
 *
 */
/**
 * Class that checks VIVO for all the publications with type as "In Process" and make a csv file.
 * @author szd2013
 * 
 */
public class ScopusInProcessUpdate {
	
	/**
	 * SLF4J Logger
	 */
	private static Logger log = LoggerFactory.getLogger(ScopusInProcessUpdate.class);
	
	
	
	/**
	 * This function will update publication types in VIVO - deleting the old one and update with the new publication type from Pubmed and Scopus
	 * @param vivoJena The connection variable to SDBJena 
	 * @param pb PublicationBean containing all the publication information 
	 * @param pubType The publication Type asserted from logic 
	 * @param namespace The default namespace to be used
	 */
	public static void updatePublicationTypesInVivo(SDBJenaConnect vivoJena, PublicationBean pb, String pubType, String namespace) {
		
		StringBuilder sb = new StringBuilder();
		//Delete the in process pub type triples from VIVO
		sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"); 
		sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n"); 
		sb.append("PREFIX bibo: <http://purl.org/ontology/bibo/> \n");
		sb.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
		sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
		sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
		sb.append("DELETE { \n");
		sb.append("<" + namespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type wcmc:InProcess . \n");
		sb.append("<" + namespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType ?type . \n");
		sb.append("} \n"); 
		sb.append("INSERT { \n");
		sb.append("<" + namespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type " + pubType + " . \n");
		sb.append("<" + namespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType " + pubType + " . \n");
		sb.append("} \n");
		sb.append("WHERE { \n");
		sb.append("<" + namespace + "pubid" + pb.getScopusDocId().trim() + "> rdf:type wcmc:InProcess . \n");
		sb.append("<" + namespace + "pubid" + pb.getScopusDocId().trim() + "> vitro:mostSpecificType ?type . \n");
		sb.append("}");
		
		log.info("Applying updates to VIVO to update publication type");
		log.info("Update Query" + sb.toString());
		
		try {
			 vivoJena.executeUpdateQuery(sb.toString(), true);
		} catch(IOException e) {
			log.error("Error Connecting to SDBJena" , e);
		}
		
		sb.setLength(0);
		log.info("Deleting any triple for in process publication type in inference graph");
		sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
		sb.append("DELETE { \n");
		sb.append("<" + namespace + "pubid" + pb.getScopusDocId().trim() + "> <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#mostSpecificType> <http://weill.cornell.edu/vivo/ontology/wcmc#InProcess> . \n");
		sb.append("} \n");
		sb.append("WHERE { \n");
		sb.append("OPTIONAL {<" + namespace + "pubid" + pb.getScopusDocId().trim() + "> <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#mostSpecificType> <http://weill.cornell.edu/vivo/ontology/wcmc#InProcess> . }\n");
		sb.append("}");
		try {
			vivoJena.executeUpdateQuery(sb.toString(), true);
		} catch(IOException e) {
			log.error("Error Connecting to SDBJena" , e);
		}
	}
	
	/**
	 * @param pubType has the publication Type to be imported in VIVO 
	 * @return the free text key word to be imported in VIVO
	 */
	private String determineFreetextKeyword(String pubType) {
		String freeTextKeyword = null;
		if(pubType.contains("Erratum")){
			freeTextKeyword = "Erratum";
		}
		else if(pubType.contains("Editorial")) {
			freeTextKeyword ="Editorial";
		}
		else if(pubType.contains("Letter")) {
			freeTextKeyword = "Letter";
		}
		else if(pubType.contains("Comment")) {
			freeTextKeyword = "Comment";
		}
		else if(pubType.contains("ConferencePaper")) {
			freeTextKeyword = "Conference Paper";
		}
		else if(pubType.contains("Review")) {
			freeTextKeyword = "Review";
		}
		else if(pubType.contains("AcademicArticle")) {
			freeTextKeyword = "Academic Article";
		}
		else if(pubType.contains("Article")) {
			freeTextKeyword = "Article";
		}
		else if(pubType.contains("NewsRelease")) {
			freeTextKeyword = "News Release";
		}
		else {
			freeTextKeyword = "In Process";
		}
		return freeTextKeyword;
	}
	
	/**
	 * @param response
	 * @param pubUrl
	 * @return The pubtype
	 */
	public static String getAndAssignPublicationType(String response) {
		Document doc = null;
		String vivoPubType = null;
		
		try {
			doc = PublicationFetch.loadXMLFromString(response);
			
			
	
		if(doc != null) {
			if (doc.getElementsByTagName("PublicationStatus").item(0).getTextContent().contains("publish")) {
				//this.pubsStatusChange.put(pmid, doc.getElementsByTagName("MedlineCitation").item(0).getAttributes().getNamedItem("Status").getTextContent());
				String pubStatus = doc.getElementsByTagName("MedlineCitation").item(0).getAttributes().getNamedItem("Status").getTextContent();
				if(pubStatus.contains("MEDLINE")) {
					if(doc.getElementsByTagName("PublicationType").getLength()<2 && doc.getElementsByTagName("PublicationType").item(0).getTextContent().contains("Journal Article")) {
						//The publication type is Journal Article alone assign publication type as "Academic Article"
						vivoPubType = "bibo:AcademicArticle";
					}
					else {
						ArrayList<String> pubType = new ArrayList<String>();
						for(int x=0; x< doc.getElementsByTagName("PublicationType").getLength(); x++) {
							pubType.add(doc.getElementsByTagName("PublicationType").item(x).getTextContent());
							
						}
							String type = pubType.get(0);
							if(type.contains("Published Erratum")) {
								vivoPubType = "vivo:Erratum";
							}
							else if(type.contains("Editorial")) {
								vivoPubType = "vivo:EditorialArticle";
							}
							else if(type.contains("Letter")) {
								vivoPubType = "vivo:Letter";
							}
							else if(type.contains("Comment")) {
								vivoPubType = "wcmc:Comment";
							}
							else if(type.contains("Addresses") || type.contains("Clinical Conference") || type.contains("Congresses") || type.contains("Consensus Development Conference, NIH") || type.contains("Lectures")) {
								vivoPubType = "vivo:ConferencePaper";
							}
							else if(type.contains("Meta-Analysis") || type.contains("Review") || type.contains("Classical Article") || type.contains("Scientific Integrity Review") || type.contains("Guideline") || type.contains("Practice Guideline")) {
								vivoPubType = "vivo:Review";
							}
							else if(type.contains("Journal Article") || type.contains("Clinical Trial") || type.contains("Clinical Trial, Phase I") || type.contains("Clinical Trial, Phase II") || type.contains("Clinical Trial, Phase III") || type.contains("Clinical Trial, Phase IV")
								|| type.contains("Randomized Controlled Trial") || type.contains("Multicenter Study") || type.contains("Twin Study") || type.contains("Validation Studies") || type.contains("Case Reports") || type.contains("Comparative Study") || type.contains("Technical Report")) {
								vivoPubType = "bibo:AcademicArticle";
							}
							else {
								vivoPubType = "bibo:Article";
							}
					}
				}
				else if(pubStatus.contains("PubMed-not-MEDLINE")) {
					//According to the logic assign "Academic Article"
					vivoPubType = "bibo:AcademicArticle";
					
				}
				else {
					//Assign it to article as was decided to no assign in process for pubmed articles
					vivoPubType = "bibo:Article";
					
				}
			}
		}
		}
		catch (Exception e) {
			log.error("extractPubmedDataFromXml Exception: ", e);
		}
		
		return vivoPubType;
	}
	
	/**
	 * this function checks the response from Scopus API and assign correct publication type according to the logic.
	 * @param response from the scopus API
	 * @param pubUrl contains the publication URL to be updated
	 */
	public static String getAndAssignPublicationTypefromScopus(String response) {
		String pubtype = null;
		Document doc = null;
		
		try {
			doc = PublicationFetch.loadXMLFromString(response);
			
			
	
		if(doc != null) {
			if (doc.getElementsByTagName("subtypeDescription").item(0).getTextContent().contains("Article in Press") && doc.getElementsByTagName("subtypeDescription").item(0) != null) {
				pubtype =  "wcmc:InProcess";
			}
			else if(doc.getElementsByTagName("subtypeDescription").item(0).getTextContent().contains("Article") && doc.getElementsByTagName("subtypeDescription").item(0) != null) {
				pubtype =  "bibo:AcademicArticle";
			}
			else if(doc.getElementsByTagName("subtypeDescription").item(0).getTextContent().contains("Conference paper") && doc.getElementsByTagName("subtypeDescription").item(0) != null) {
				pubtype = "vivo:ConferencePaper";
			}
			else if(doc.getElementsByTagName("subtypeDescription").item(0).getTextContent().contains("Editorial") && doc.getElementsByTagName("subtypeDescription").item(0) != null) {
				pubtype = "vivo:EditorialArticle";
			}
			else if(doc.getElementsByTagName("subtypeDescription").item(0).getTextContent().contains("Letter") && doc.getElementsByTagName("subtypeDescription").item(0) != null) {
				pubtype = "vivo:Letter";
			}
			else if(doc.getElementsByTagName("subtypeDescription").item(0).getTextContent().contains("Note") && doc.getElementsByTagName("subtypeDescription").item(0) != null) {
				pubtype = "wcmc:Comment";
			}
			else if(doc.getElementsByTagName("subtypeDescription").item(0).getTextContent().contains("Press release") && doc.getElementsByTagName("subtypeDescription").item(0) != null) {
				pubtype = "vivo:NewsRelease";
			}
			else if(doc.getElementsByTagName("subtypeDescription").item(0).getTextContent().contains("Report") && doc.getElementsByTagName("subtypeDescription").item(0) != null) {
				pubtype = "bibo:Report";
			}
			else if(doc.getElementsByTagName("subtypeDescription").item(0).getTextContent().contains("Book") && doc.getElementsByTagName("subtypeDescription").item(0) != null) {
				pubtype = "bibo:Book";
			}
			else if(doc.getElementsByTagName("subtypeDescription").item(0).getTextContent().contains("Chapter") && doc.getElementsByTagName("subtypeDescription").item(0) != null) {
				pubtype = "bibo:Chapter";
			}
			else {
				pubtype = "bibo:Article";
			}
				
			
		}
		}
		catch (Exception e) {
			log.error("extractPubmedDataFromXml Exception: ", e);
		}
		return pubtype;
	}
	
	/**
	 * @param con
	 * @param pb
	 * @param pubType
	 */
	public static void updatePubadmin(Connection con, PublicationBean pb, String pubType) {
		String updateQuery = null;
		PreparedStatement ps = null;
		String pubAdminPubType = null;
		
		if(pubType !=null && !pubType.isEmpty()) {
			if(pubType.equals("wcmc:InProcess"))
				pubAdminPubType = "ip";
			else if(pubType.equals("bibo:AcademicArticle"))
				pubAdminPubType = "ar";
			else if(pubType.equals("bibo:Report"))
				pubAdminPubType = "re";
			else if(pubType.equals("vivo:ConferencePaper"))
				pubAdminPubType = "cp";
			else if(pubType.equals("vivo:Letter"))
				pubAdminPubType = "le";
			else if(pubType.equals("wcmc:Comment"))
				pubAdminPubType = "no";
			else if(pubType.equals("vivo:EditorialArticle"))
				pubAdminPubType = "ed";
			else if(pubType.equals("bibo:Article"))
				pubAdminPubType = "Journal Article";
			else if(pubType.equals("vivo:NewsRelease"))
				pubAdminPubType = "pr";
			else if(pubType.equals("vivo:Review"))
				pubAdminPubType = "Review";
			else if(pubType.equals("bibo:Book"))
				pubAdminPubType = "bk";
			else if(pubType.equals("bibo:Chapter"))
				pubAdminPubType = "ch";
			
			
		}
		if(pubAdminPubType != null) {
			updateQuery = "update wcmc_document set vivo_ingest_date_history=NOW(), pubtype = '" + pubAdminPubType + "' where wcmc_document_pk = " + pb.getPublicationId();

			try {
				ps = con.prepareStatement(updateQuery);
				log.info(ps.executeUpdate() + " row updated in pubadmin for wcmc_document_pk with current publication type from in process : " + pb.getPublicationId());
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
	
}
