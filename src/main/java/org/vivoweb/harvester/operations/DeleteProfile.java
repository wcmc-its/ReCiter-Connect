package org.vivoweb.harvester.operations;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.openjena.atlas.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.vivoweb.harvester.ingest.EdDataInterface;
import org.vivoweb.harvester.ingest.EdDataInterfaceImpl;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;

import lombok.extern.slf4j.Slf4j;

import org.json.JSONArray;
import org.json.JSONObject;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.SearchResultEntry;

import reciter.connect.beans.vivo.delete.profile.PublicationBean;
import reciter.connect.database.ldap.LDAPConnectionFactory;
import reciter.connect.database.mysql.MysqlConnectionFactory;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;
import reciter.connect.vivo.IngestType;
import reciter.connect.vivo.api.client.VivoClient;



/**
 * This class delete profile for those persons who have become inactive and therefore has to be deleted from VIVO.
 * It checks for all the publications authored by the person and checks whether those publication have any additional WCMC authors. If yes it does not mark it for deletion but for only 
 * the inactive authored pubs the publications are deleted. The next steps would be deleting all the information related to that person from VIVO and create an external entity in application
 * and add that personid as authorship to the publication with multiple WCMC authors.
 * @author szd2013
 * Date - 10/24/2016
 *
 */
@Component
public class DeleteProfile {
	
	public static String propertyFilePath = null;
	private static Properties props = new Properties();
	
	private static String ldapbasedn = null;
	
	private String givenName = null;
	private String familyName = null;
	private String middleName = null;
	
	/**
	 * <i>This is the global connection variable for all connections to PubAdmin</i>
	 */
	private Connection con = null;
	
	/**
	 * Jena connection factory object for all the apache jena sdb related connections
	 */
	@Autowired
	private JenaConnectionFactory jcf;
	
	/**
	 * The default namespace for VIVO
	 */
	private String vivoNamespace = JenaConnectionFactory.nameSpace;
	
	/**
	 * <i>This is a connection factory object to get ldap connection to Enterprise Directory</i>
	 */
	@Autowired
	private LDAPConnectionFactory lcf;

	@Autowired
	private EdDataInterface edi;

	private String ingestType = System.getenv("INGEST_TYPE");

	@Autowired
	private VivoClient vivoClient;
	
	
	/**
	 * SLF4J Logger
	 */
	private static Logger logger = LoggerFactory.getLogger(DeleteProfile.class);
	
	/**
	 * @param cwid which is supplied as an argument
	 * This method get lists of grants(if any) for the cwid
	 * @param grants Stores all the grants with roles for a cwid
	 */
	private void getListOfGrants(String cwid, Map<String, String> grants) {
		
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT distinct ?grant ?role \n");
		sb.append("WHERE { \n");
		sb.append("GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> {\n");
		sb.append("<" + this.vivoNamespace + "cwid-" + cwid +"> <http://purl.obolibrary.org/obo/RO_0000053> ?role . \n");
		sb.append("?role <http://vivoweb.org/ontology/core#relatedBy> ?grant . \n");
		sb.append("}}");
		
		
		if(ingestType.equals(IngestType.VIVO_API.toString())) {
			try {
				String response = this.vivoClient.vivoQueryApi(sb.toString());
				logger.info(response);
				JSONObject obj = new JSONObject(response);
				JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
				if(bindings != null && !bindings.isEmpty()) {
					for (int i = 0; i < bindings.length(); ++i) {
						if(bindings.getJSONObject(i).optJSONObject("grant") != null && bindings.getJSONObject(i).optJSONObject("grant").has("value")
						&&
						bindings.getJSONObject(i).optJSONObject("role") != null && bindings.getJSONObject(i).optJSONObject("role").has("value")) {
							grants.put("<" + bindings.getJSONObject(i).getJSONObject("grant").getString("value") + ">", "<" + bindings.getJSONObject(i).getJSONObject("role").getString("value") + ">");
						}
					}
				}
			} catch(Exception e) {
				logger.error("Api Exception", e);
			}
		} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
		
			SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
			ResultSet rs = null;
			try {
				rs = vivoJena.executeSelectQuery(sb.toString(), true);
			
			logger.info("Grant List for cwid " + cwid);
			while(rs.hasNext())
			{
				QuerySolution qs =rs.nextSolution();
				
				logger.info("Grant - " + qs.get("grant").toString() + " Role - " + qs.get("role").toString());
				if(qs.get("grant") != null && qs.get("role") != null) 
					grants.put("<" + qs.get("grant").toString() + ">", "<" + qs.get("role").toString() + ">");
				
				//logger.info("Grant - " + qs.get("grant").toString() + " with role " +  qs.get("role").toString());
				
			}
			} catch(IOException e) {
				logger.error("Error Connecting to Jena Database" , e);
			}
			this.jcf.returnConnectionToPool(vivoJena, "dataSet");
		}
		
		
	}
	
	
	
	/**
	 * @param cwid which is supplied as an argument
	 * This method lists all the publications for the cwid supplied as an argument
	 * @param publications This parameter stores all the publications for the inactive faculty
	 */
	private void getListofPublications(String cwid, List<PublicationBean> publications)  {

		String sparqlQuery = "PREFIX vivo: <http://vivoweb.org/ontology/core#> \n" + 
			 "PREFIX bibo: <http://purl.org/ontology/bibo/> \n" +
			 "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" + 
			 "SELECT distinct ?pub ?Authorship \n" +
			 "WHERE { \n"+
			 "GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> {\n" +
			 "<" + this.vivoNamespace + "cwid-" + cwid +"> ?p ?Authorship . \n" +
			 "?Authorship vivo:relates ?pub . \n" +
			 "?pub rdf:type bibo:Document . \n" +
			 "FILTER(!REGEX(STR(?pub),\"http://xmlns.com/foaf/0.1/Person\",\"i\")) \n" +
			 "}}";
		
		if(ingestType.equals(IngestType.VIVO_API.toString())) {
			try {
				String response = this.vivoClient.vivoQueryApi(sparqlQuery);
				logger.info(response);
				JSONObject obj = new JSONObject(response);
				JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
				if(bindings != null && !bindings.isEmpty()) {
					for (int i = 0; i < bindings.length(); ++i) {
						PublicationBean pb = new PublicationBean();
						if(bindings.getJSONObject(i).optJSONObject("pub") != null && bindings.getJSONObject(i).optJSONObject("pub").has("value")) {
							pb.setPubUrl(bindings.getJSONObject(i).getJSONObject("pub").getString("value"));
						}
						if(bindings.getJSONObject(i).optJSONObject("Authorship") != null && bindings.getJSONObject(i).optJSONObject("Authorship").has("value")) {
							pb.setAuthorshipUrl(bindings.getJSONObject(i).getJSONObject("Authorship").getString("value"));
						}
						pb.setAuthorUrl(this.vivoNamespace + "cwid-" + cwid.trim());
						publications.add(pb);
					}
				}
			} catch(Exception e) {
				logger.error("Api Exception", e);
			}
		} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
		
			SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
			ResultSet rs;
			try {
				rs = vivoJena.executeSelectQuery(sparqlQuery, true);
			
			logger.info("Publication list for cwid " + cwid);
			while(rs.hasNext())
			{
				QuerySolution qs =rs.nextSolution();
				PublicationBean pb = new PublicationBean();
				if(qs.get("pub") != null) {
					pb.setPubUrl(qs.get("pub").toString());
				}
				if(qs.get("Authorship") != null) {
					pb.setAuthorshipUrl(qs.get("Authorship").toString());
				}
				pb.setAuthorUrl(this.vivoNamespace + "cwid-" + cwid.trim());
				publications.add(pb);
				
				logger.info("Publication URI - " + pb.getPubUrl());
			}
			} catch(IOException e) {
				logger.error("Error Connecting to Jena Database" , e);
			}
			this.jcf.returnConnectionToPool(vivoJena, "dataSet");
		}
		
		
		
	}
	
	
	/**
	 * This method will check from the list of publications published by the individual to see any of the publication has any other additional WCMC authors. 
	 */
	private  void checkAdditionalWCMCAuthoredPubs( List<PublicationBean> publications) {
		
		Iterator<PublicationBean> i = publications.iterator();
		logger.info("Check for Additional WCMC Authored pubs.");
		while(i.hasNext())
		{
			PublicationBean pub = i.next();
			String sparqlQuery = "PREFIX vivo: <http://vivoweb.org/ontology/core#> \n" + 
				 "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" + 
				 "SELECT (count(?AuthorCount) as ?count) \n" +
				 "WHERE { \n"+
				 "GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> {\n" +
				 "<" + pub.getPubUrl() + "> vivo:relatedBy ?authors . \n" +
				 "?authors vivo:relates ?AuthorCount . \n" +
				 "FILTER(REGEX(STR(?AuthorCount),\"cwid\",\"i\")) \n" +
				 "}}";
			
			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				try {
					String response = this.vivoClient.vivoQueryApi(sparqlQuery);
					logger.info(response);
					JSONObject obj = new JSONObject(response);
					JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
					if(bindings != null && !bindings.isEmpty()) {
						int count = bindings.getJSONObject(0).getJSONObject("count").getInt("value");
						if(count <= 1) 
							pub.setAdditionalWcmcAuthorFlag(false);
						else
							pub.setAdditionalWcmcAuthorFlag(true);
					}
				} catch(Exception e) {
					logger.error("Api Exception", e);
				}
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
				ResultSet rs;
				try {
					rs = vivoJena.executeSelectQuery(sparqlQuery, true);
					int count = Integer.parseInt(rs.nextSolution().get("count").toString().replace("^^http://www.w3.org/2001/XMLSchema#integer", ""));
					if(count <= 1) 
						pub.setAdditionalWcmcAuthorFlag(false);
					else
						pub.setAdditionalWcmcAuthorFlag(true);
				} catch(IOException e) {
					logger.error("Error Connecting to Jena Database" , e);
				}
				this.jcf.returnConnectionToPool(vivoJena, "dataSet");
			}
			
			
		}
		
		
		
	}
	
	
	
	/**
	 * @param cwid contains the cwid of the profile to be delete in VIVO
	 * This function will delete the entire dataset related to the cwid supplied in Virtuoso
	 * @param publications Stores all the publication for inactive faculty
	 * @throws IOException exception thrown by JenaConnect
	 */
	private void deleteProfile(String cwid, List<PublicationBean> publications, Map<String, String> grants) throws IOException {
		String sparql = null;
		SDBJenaConnect vivoJena = null;
		
		if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
			vivoJena = this.jcf.getConnectionfromPool("dataSet");
		}

			// Delete from People Graph
			logger.info("Deleting profile in People graph for " + cwid );
			sparql = "WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> \n" +
				"DELETE { \n" +
				"<" + this.vivoNamespace + "cwid-" + cwid + "> ?p ?o . \n" +
				"<" + this.vivoNamespace + "hasTitle-" + cwid + "> ?p ?o . \n" +
				"<" + this.vivoNamespace + "hasName-" + cwid + "> ?p ?o . \n" +
				"<" + this.vivoNamespace + "hasEmail-" + cwid + "> ?p ?o . \n" +
				"<" + this.vivoNamespace + "arg2000028-" + cwid + "> ?p ?o . \n" +
				
				"} WHERE { \n" +
				"<" + this.vivoNamespace + "cwid-" + cwid + "> ?p ?o . \n" +
				"OPTIONAL { <" + this.vivoNamespace + "hasTitle-" + cwid + "> ?p ?o . }\n" +
				"OPTIONAL { <" + this.vivoNamespace + "hasName-" + cwid + "> ?p ?o . }\n" +
				"OPTIONAL { <" + this.vivoNamespace + "hasEmail-" + cwid + "> ?p ?o . }\n" +
				"OPTIONAL { <" + this.vivoNamespace + "arg2000028-" + cwid + "> ?p ?o . }\n" +
				"}";

			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				logger.info(this.vivoClient.vivoUpdateApi(sparql));
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				vivoJena.executeUpdateQuery(sparql, true);
			}
			
			
			logger.info("Deleting inference triples in People graph for " + cwid );
			sparql = "WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> \n" +
				"DELETE { \n" +
				 "?s ?p <" + this.vivoNamespace + "cwid-" + cwid + "> . \n" +
				 "} WHERE { \n" +
				 "OPTIONAL {?s ?p <" + this.vivoNamespace + "cwid-" + cwid + "> . }\n" +
				 "}";
			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				logger.info(this.vivoClient.vivoUpdateApi(sparql));
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				vivoJena.executeUpdateQuery(sparql, true);
			}
			
			//Deleting from Ofa graph
			
			logger.info("Deleting position and educational background in Ofa graph");
			sparql = "WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n" +
				"DELETE { \n" +
				"<" + this.vivoNamespace + "cwid-" + cwid + "> <http://vivoweb.org/ontology/core#relatedBy> ?position . \n" +
				"?position ?positionpred ?positionobj . \n" +
				"} WHERE { \n" +
				"<" + this.vivoNamespace + "cwid-" + cwid + "> <http://vivoweb.org/ontology/core#relatedBy> ?position . \n" +
				"OPTIONAL {?position ?positionpred ?positionobj . }\n" +
				"}";
			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				logger.info(this.vivoClient.vivoUpdateApi(sparql));
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				vivoJena.executeUpdateQuery(sparql, true);
			}
			
			logger.info("Deleting profile in Ofa graph for " + cwid );
			sparql = "WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa> \n" +
				"DELETE { \n" +
				"<" + this.vivoNamespace + "cwid-" + cwid + "> ?p ?o . \n" +
				"} WHERE { \n" +
				"<" + this.vivoNamespace + "cwid-" + cwid + "> ?p ?o . \n" +
				"}";
			
			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				logger.info(this.vivoClient.vivoUpdateApi(sparql));
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				vivoJena.executeUpdateQuery(sparql, true);
			}
			
			//Deleting from kb-2 graph
			logger.info("Deleting profile in kb-2 graph for " + cwid );
			sparql = "WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-2> \n" +
				"DELETE { \n" +
				"<" + this.vivoNamespace + "cwid-" + cwid + "> ?p ?o . \n" +
				"} WHERE { \n" +
				"OPTIONAL {<" + this.vivoNamespace + "cwid-" + cwid + "> ?p ?o .}\n" +
				"}";
			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				logger.info(this.vivoClient.vivoUpdateApi(sparql));
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				vivoJena.executeUpdateQuery(sparql, true);
			}
			
			logger.info("Deleting inference triples in kb-2 graph for " + cwid );
			sparql = "WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-2> \n" +
				"DELETE { \n" +
				 "?s ?p <" + this.vivoNamespace + "cwid-" + cwid + "> . \n" +
				 "} WHERE { \n" +
				 "OPTIONAL {?s ?p <" + this.vivoNamespace + "cwid-" + cwid + "> . }\n" +
				 "}";

			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				logger.info(this.vivoClient.vivoUpdateApi(sparql));
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				vivoJena.executeUpdateQuery(sparql, true);
			}
			
			logger.info("Deleting manually added triples from kb-2 graph for " + cwid);
			sparql = "SELECT ?obj \n" +
					 "WHERE { \n" +
					 "GRAPH <http://vitro.mannlib.cornell.edu/default/vitro-kb-2> {\n" +
					 "<" + this.vivoNamespace + "cwid-" + cwid + "> <http://vivoweb.org/ontology/core#relatedBy> ?obj . \n" +
					 "}}";
			
			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				try {
					String response = this.vivoClient.vivoQueryApi(sparql);
					logger.info(response);
					JSONObject obj = new JSONObject(response);
					JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
					if(bindings != null && !bindings.isEmpty()) {
						for (int i = 0; i < bindings.length(); ++i) {
							if(bindings.getJSONObject(i).optJSONObject("obj") != null && bindings.getJSONObject(i).optJSONObject("obj").has("value")) {
								String manual = bindings.getJSONObject(i).getJSONObject("obj").getString("value");
								sparql = "WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-2> \n" +
								"DELETE { \n" +
								"<" + manual + "> ?p ?o . \n" +
								"} WHERE { \n" +
								"<" + manual + "> ?p ?o . \n" +
								"}";
								logger.info(this.vivoClient.vivoUpdateApi(sparql));
							}
						}
					}
				} catch(Exception e) {
					logger.error("Api Exception", e);
				}
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				ResultSet rs = vivoJena.executeSelectQuery(sparql, true);
				while(rs.hasNext())
				{
					QuerySolution qs =rs.nextSolution();
					if(qs.get("obj") != null) {
						String manual = qs.get("obj").toString().trim();
						
						sparql = "WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-2> \n" +
							"DELETE { \n" +
							"<" + manual + "> ?p ?o . \n" +
							"} WHERE { \n" +
							"<" + manual + "> ?p ?o . \n" +
							"}";
						vivoJena.executeUpdateQuery(sparql, true);	
					}	
				}
			}
			
			sparql = "SELECT ?obj \n" +
				 "WHERE { \n" +
				 "GRAPH <http://vitro.mannlib.cornell.edu/default/vitro-kb-2> {\n" +
				 "<" + this.vivoNamespace + "cwid-" + cwid + "> <http://vitro.mannlib.cornell.edu/ns/vitro/public#mainImage> ?obj . \n" +
				 "}}";
			
			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				try {
					String response = this.vivoClient.vivoQueryApi(sparql);
					logger.info(response);
					JSONObject obj = new JSONObject(response);
					JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
					if(bindings != null && !bindings.isEmpty()) {
						for (int i = 0; i < bindings.length(); ++i) {
							if(bindings.getJSONObject(i).optJSONObject("obj") != null && bindings.getJSONObject(i).optJSONObject("obj").has("value")) {
								String manual = bindings.getJSONObject(i).getJSONObject("obj").getString("value");
								sparql = "WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-2> \n" +
								"DELETE { \n" +
								"<" + manual + "> ?p ?o . \n" +
								"} WHERE { \n" +
								"<" + manual + "> ?p ?o . \n" +
								"}";
								logger.info(this.vivoClient.vivoUpdateApi(sparql));
							}
						}
					}
				} catch(Exception e) {
					logger.error("Api Exception", e);
				}
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				ResultSet rs = vivoJena.executeSelectQuery(sparql, true);
				while(rs.hasNext())
				{
					QuerySolution qs =rs.nextSolution();
					if(qs.get("obj") != null) {
						String manual = qs.get("obj").toString().trim();
						
						sparql = "WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-2> \n" +
							"DELETE { \n" +
							"<" + manual + "> ?p ?o . \n" +
							"} WHERE { \n" +
							"<" + manual + "> ?p ?o . \n" +
							"}";
						vivoJena.executeUpdateQuery(sparql, true);	
					}	
				}
			}
			
			logger.info("Deleting profile in inference graph for " + cwid );
			sparql = "WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> \n" +
				"DELETE { \n" +
				"<" + this.vivoNamespace + "cwid-" + cwid + "> ?p ?o . \n" +
				"} WHERE { \n" +
				"OPTIONAL {<" + this.vivoNamespace + "cwid-" + cwid + "> ?p ?o .}\n" +
				"}";
			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				logger.info(this.vivoClient.vivoUpdateApi(sparql));
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				vivoJena.executeUpdateQuery(sparql, true);
			}
			
			logger.info("Deleting inference triples in kb-inf graph for " + cwid );
			sparql = "WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> \n" +
				"DELETE { \n" +
				 "?s ?p <" + this.vivoNamespace + "cwid-" + cwid + "> . \n" +
				 "} WHERE { \n" +
				 "OPTIONAL {?s ?p <" + this.vivoNamespace + "cwid-" + cwid + "> .} \n" +
				 "}";
			
			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				logger.info(this.vivoClient.vivoUpdateApi(sparql));
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				vivoJena.executeUpdateQuery(sparql, true);
			}
		
		if(publications != null && !publications.isEmpty()) {
		
		
		Iterator<PublicationBean> i = publications.iterator();
		
			while(i.hasNext()) {
				PublicationBean pub = i.next();
				if(!pub.isAdditionalWcmcAuthorFlag()) {
					logger.info("Deleting publication - " + pub.getPubUrl());
					sparql = "WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n" +
									"DELETE { \n" +
									"<" + pub.getPubUrl().trim() + ">  ?p ?o . \n" +
									"<" + this.vivoNamespace + "citation-" + pub.getPubUrl().trim().replace(this.vivoNamespace, "") + "> ?p1 ?o1 .\n" +
									"} WHERE { \n" +
									"<" + pub.getPubUrl().trim() + ">  ?p ?o .\n" +
									"OPTIONAL { <" + this.vivoNamespace + "citation-" + pub.getPubUrl().trim().replace(this.vivoNamespace, "") + "> ?p1 ?o1 .}\n" +
									"}";
					if(ingestType.equals(IngestType.VIVO_API.toString())) {
						logger.info(this.vivoClient.vivoUpdateApi(sparql));
					} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
						vivoJena.executeUpdateQuery(sparql, true);
					}
					
					sparql = "WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n" +
						"DELETE { \n" +
						"?s ?p <" +pub.getPubUrl().trim() + "> . \n" +
						"} WHERE { \n" +
						"OPTIONAL {?s ?p <" +pub.getPubUrl().trim() + "> .}\n" +
						"}";
					if(ingestType.equals(IngestType.VIVO_API.toString())) {
						logger.info(this.vivoClient.vivoUpdateApi(sparql));
					} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
						vivoJena.executeUpdateQuery(sparql, true);
					}
					
					sparql = "WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> \n" +
						"DELETE { \n" +
						"<" + pub.getPubUrl().trim() + ">  ?p ?o .\n" +
						"} WHERE { \n" +
						"<" + pub.getPubUrl().trim() + ">  ?p ?o . \n" +
						"}";
					if(ingestType.equals(IngestType.VIVO_API.toString())) {
						logger.info(this.vivoClient.vivoUpdateApi(sparql));
					} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
						vivoJena.executeUpdateQuery(sparql, true);
					}
				}
			}
				
			logger.info("Deleting profile in Publications graph for " + cwid );
			sparql = "WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n" +
				"DELETE { \n" +
				"<" + this.vivoNamespace + "cwid-" + cwid + "> ?p ?o . \n" +
				"} WHERE { \n" +
				"<" + this.vivoNamespace + "cwid-" + cwid + "> ?p ?o . \n" +
				"}";
			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				logger.info(this.vivoClient.vivoUpdateApi(sparql));
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				vivoJena.executeUpdateQuery(sparql, true);
			}
			
			
			
			logger.info("Deleting inference triples in Publications graph for " + cwid );
			sparql = "WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n" +
				"DELETE { \n" +
				 "?s ?p <" + this.vivoNamespace + "cwid-" + cwid + "> . \n" +
				 "} WHERE { \n" +
				 "OPTIONAL {?s ?p <" + this.vivoNamespace + "cwid-" + cwid + "> .}\n" +
				"}";
			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				logger.info(this.vivoClient.vivoUpdateApi(sparql));
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				vivoJena.executeUpdateQuery(sparql, true);
			}
		}
		
		if(!grants.isEmpty()) {
			//delete contributor from grants and entry for grant in the profile
			logger.info("Deleting triples for grants in wcmcCoeus graph");
			String grant;
			String role;
			int count = 0;
			StringBuilder sb = new StringBuilder();
			if(grants.size() > 5) {
				Iterator<Entry<String, String>> it = grants.entrySet().iterator();
				while(it.hasNext()) {
					Entry<String, String> pair = it.next();
					grant = pair.getKey();
					role = pair.getValue();
					sb = new StringBuilder();
					sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> \n");
					sb.append("DELETE { \n");
					sb.append("<" + this.vivoNamespace + "cwid-" + cwid + "> ?pred ?obj . \n");
					sb.append(grant + " <http://vivoweb.org/ontology/core#relates> <" + this.vivoNamespace + "cwid-" + cwid + "> . \n");
					sb.append(grant + " <http://vivoweb.org/ontology/core#relates> " + role + " . \n");
					sb.append(role + " ?p ?o . \n");
					sb.append("} WHERE { \n");
					sb.append("OPTIONAL {<" + this.vivoNamespace + "cwid-" + cwid + "> ?pred ?obj . }\n");
					sb.append("OPTIONAL { " + grant + " <http://vivoweb.org/ontology/core#relates> <" + this.vivoNamespace + "cwid-" + cwid + "> . }\n");
					sb.append("OPTIONAL { " + grant + " <http://vivoweb.org/ontology/core#relates> " + role + " . }\n");
					sb.append("OPTIONAL { " + role + " ?p ?o . }\n");
					sb.append("}");
					
					if(ingestType.equals(IngestType.VIVO_API.toString())) {
						logger.info(this.vivoClient.vivoUpdateApi(sb.toString()));
					} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
						vivoJena.executeUpdateQuery(sb.toString(), true);
					}
					sb.setLength(0);
					
				}
				
			}
			else {

				
				
				sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> \n");
				sb.append("DELETE { \n");
				sb.append("<" + this.vivoNamespace + "cwid-" + cwid + "> ?pred ?obj . \n");
				Iterator<Entry<String, String>> it = grants.entrySet().iterator();
				count = 1;
				while(it.hasNext()) {
					Entry<String, String> pair = it.next();
					grant = pair.getKey();
					role = pair.getValue();
					sb.append(grant + " <http://vivoweb.org/ontology/core#relates> <" + this.vivoNamespace + "cwid-" + cwid + "> . \n");
					sb.append(grant + " <http://vivoweb.org/ontology/core#relates> " + role + " . \n");
					sb.append(role + " ?p" + count +" ?o" + count + " . \n");
					count = count + 1;
				}
				sb.append("} WHERE { \n");
				sb.append("<" + this.vivoNamespace + "cwid-" + cwid + "> ?pred ?obj . \n");
				Iterator<Entry<String, String>> it1 = grants.entrySet().iterator();
				count = 1;
				while(it1.hasNext()) {
					Entry<String, String> pair = it1.next();
					grant = pair.getKey();
					role = pair.getValue();
					sb.append("OPTIONAL { " + grant + " <http://vivoweb.org/ontology/core#relates> <" + this.vivoNamespace + "cwid-" + cwid + "> . }\n");
					sb.append("OPTIONAL { " + grant + " <http://vivoweb.org/ontology/core#relates> " + role + " . }\n");
					sb.append("OPTIONAL { " + role + " ?p" + count +" ?o" + count + " . }\n");
					count = count + 1;
				}
				sb.append("}");
				
				if(ingestType.equals(IngestType.VIVO_API.toString())) {
					logger.info(this.vivoClient.vivoUpdateApi(sb.toString()));
				} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
					vivoJena.executeUpdateQuery(sb.toString(), true);
				}
				
			}
			
			sb.setLength(0);
			logger.info("Deleting inference triples for grants");
			if(grants.size() > 5) {
				Iterator<Entry<String, String>> it = grants.entrySet().iterator();
				while(it.hasNext()) {
					Entry<String, String> pair = it.next();
					grant = pair.getKey();
					role = pair.getValue();
					sb = new StringBuilder();
					sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus> \n");
					sb.append("DELETE { \n");
					sb.append(role + " ?p ?o . \n");
					sb.append("} WHERE { \n");
					sb.append("OPTIONAL { " + role + " ?p ?o . }\n");
					sb.append("}");
					
					if(ingestType.equals(IngestType.VIVO_API.toString())) {
						logger.info(this.vivoClient.vivoUpdateApi(sb.toString()));
					} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
						vivoJena.executeUpdateQuery(sb.toString(), true);
					}
					sb.setLength(0);
					
					
				}
			}
			else {
				
				sb.append("WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> \n");
				sb.append("DELETE { \n");
				Iterator<Entry<String, String>> it2 = grants.entrySet().iterator();
				count = 1;
				while(it2.hasNext()) {
					Entry<String, String> pair = it2.next();
					grant = pair.getKey();
					role = pair.getValue();
					sb.append(role + " ?p" + count +" ?o" + count + " . \n");
					count = count + 1;
				}
				sb.append("} WHERE { \n");
				Iterator<Entry<String, String>> it3 = grants.entrySet().iterator();
				count = 1;
				while(it3.hasNext()) {
					Entry<String, String> pair = it3.next();
					grant = pair.getKey();
					role = pair.getValue();
					sb.append("OPTIONAL { " + role + " ?p" + count +" ?o" + count + " . }\n");
					count = count + 1;
				}
				sb.append("}");
				
				if(ingestType.equals(IngestType.VIVO_API.toString())) {
					logger.info(this.vivoClient.vivoUpdateApi(sb.toString()));
				} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
					vivoJena.executeUpdateQuery(sb.toString(), true);
				}
				
			}	
		}
		if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
			this.jcf.returnConnectionToPool(vivoJena, "dataSet");
		}
		
	}
	
	/**
	 * @param cwid The unique identifier to search for in ED
	 * @return whether the person is active in ED or not
	 * This function checks for a supplied cwid whether the person is active in ED or not
	 */
	public boolean checkForInActivePeopleEd(String cwid) {
		boolean isActive = false;
		LDAPConnection connection = this.lcf.getConnectionfromPool();
		if(connection != null) {
			this.lcf.returnConnectionToPool(connection);
			List<SearchResultEntry> results = this.lcf.searchWithBaseDN("(&(objectClass=eduPerson)(weillCornellEduCWID=" + cwid + "))", "ou=people,dc=weill,dc=cornell,dc=edu");
		
			if (results.size() == 1) {
				SearchResultEntry entry = results.get(0);
				if(entry.getAttributeValue("weillCornellEduCWID") != null) {
					if(entry.getAttributeValues("weillCornellEduPersonTypeCode") != null) {
						String personType[] = new String[entry.getAttributeValues("weillCornellEduPersonTypeCode").length];
						personType = entry.getAttributeValues("weillCornellEduPersonTypeCode");
						List<String> ptypes = Arrays.asList(personType);
						if(ptypes.contains("academic")) 
							isActive = true;
						else
							isActive = false;
					}
					else
						isActive = false;
					
					if(entry.getAttributeValue("sn") !=null ) 
						this.familyName = StringEscapeUtils.escapeJava(entry.getAttributeValue("sn"));
					
					if(entry.getAttributeValue("givenName") !=null )
						this.givenName = StringEscapeUtils.escapeJava(entry.getAttributeValue("givenName"));
					
					if(entry.getAttributeValue("weillCornellEduMiddleName") != null)
						this.middleName = " " + StringEscapeUtils.escapeJava(entry.getAttributeValue("weillCornellEduMiddleName")) + " ";
					else
						this.middleName= " ";
					
					//logger.info(entry.toLDIFString());

				}
				else
					isActive = false;
				
			}
			else
				isActive = false;
			
			if(isActive==false) {
				if(this.givenName == null && this.familyName == null)
				getNamesFromVivo(cwid);
			}
		} else 
			isActive = true;
		return isActive;
	}
	
	/**
	 * @param cwid Unique identifier 
	 * This function gets the full Name for the faculty
	 */
	private void getNamesFromVivo(String cwid) {
		String sparqlQuery = "SELECT ?givenName ?familyName \n" +
			 "WHERE { \n" +
			 "GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> {\n" +
			 "<" + this.vivoNamespace + "hasName-" + cwid.trim() + "> <http://www.w3.org/2006/vcard/ns#givenName> ?givenName . \n" +
			 "<" + this.vivoNamespace + "hasName-" + cwid.trim() + "> <http://www.w3.org/2006/vcard/ns#familyName> ?familyName . \n" +
			 "}}";
		
		if(ingestType.equals(IngestType.VIVO_API.toString())) {
			String response = vivoClient.vivoQueryApi(sparqlQuery);
			logger.info(response);
			JSONObject obj = new JSONObject(response);
			JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
			if(bindings != null && !bindings.isEmpty()) {
				if(bindings.getJSONObject(0).optJSONObject("givenName") != null && bindings.getJSONObject(0).optJSONObject("givenName").has("value")
				&&
				bindings.getJSONObject(0).optJSONObject("familyName") != null && bindings.getJSONObject(0).optJSONObject("familyName").has("value")) {
					this.givenName = bindings.getJSONObject(0).getJSONObject("givenName").getString("value");
					this.familyName = bindings.getJSONObject(0).getJSONObject("familyName").getString("value");
				}
				
			}
		} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
		
			SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
			ResultSet rs;
			
			try {
				rs = vivoJena.executeSelectQuery(sparqlQuery, true);
				if(rs != null && rs.hasNext()) {
					QuerySolution qs = rs.nextSolution();
					this.givenName = qs.get("givenName").toString().trim();
					this.familyName = qs.get("familyName").toString().trim();
				}
			} catch(IOException e) {
			logger.error("Error connecting to Jena Database" , e);
			}
			this.jcf.returnConnectionToPool(vivoJena, "dataSet");
		}
		
		if(this.givenName==null && this.familyName==null) {
			sparqlQuery = "SELECT ?label \n" +
				 "WHERE { \n" +
				 "GRAPH <http://vitro.mannlib.cornell.edu/default/vitro-kb-2> {\n" +
				 "<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> <http://www.w3.org/2000/01/rdf-schema#label> ?label . \n" +
				 "}}";
			
			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				String response = vivoClient.vivoQueryApi(sparqlQuery);
				logger.info(response);
				JSONObject obj = new JSONObject(response);
				JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
				if(bindings != null && !bindings.isEmpty()) {
					if(bindings.getJSONObject(0).optJSONObject("label") != null && bindings.getJSONObject(0).optJSONObject("label").has("value")) {
						String label =bindings.getJSONObject(0).getJSONObject("label").getString("value").replace("@en-us", "").replace("\"", "").trim();
						String[] splitLabel = label.split(",");
						this.givenName = splitLabel[0].trim();
						this.familyName = splitLabel[1].trim();
					}
					
				}
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
			
				SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
				
				try {
					ResultSet rs = vivoJena.executeSelectQuery(sparqlQuery, true);
					if(rs != null && rs.hasNext()) {
						QuerySolution qs = rs.nextSolution();
						String label = qs.get("label").toString().replace("@en-us", "").replace("\"", "").trim();
						String[] splitLabel = label.split(",");
						
						this.givenName = splitLabel[0].trim();
						this.familyName = splitLabel[1].trim();
					}
				} catch(IOException e) {
				logger.error("Error connecting to Jena Database" , e);
				}
				this.jcf.returnConnectionToPool(vivoJena, "dataSet");
			}
		}
		
	}
	
	
	/**
	 * @param newUri The uri for external entity
	 * @return check whether the random number generated already exist in VIVO
	 */
	private boolean isNotInVivo(String newUri) {
		boolean inVivo = false;
		String sparqlQuery = "SELECT (count(?s) as ?count) \n" +
			 "WHERE { \n" +
			 "GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> {\n" +
			 "<" + newUri.trim() + "> ?p ?o . \n" +
			 "}}";
		
		logger.info(sparqlQuery);
		
		
		SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
		ResultSet rs;
		
		try {
		rs = vivoJena.executeSelectQuery(sparqlQuery, true);
		int count = Integer.parseInt(rs.nextSolution().get("count").toString().replace("^^http://www.w3.org/2001/XMLSchema#integer", ""));
		if(count == 0) 
		inVivo = false;
		
		} catch(IOException e) {
		logger.error("Error connecting to Jena Database" , e);
		}
		this.jcf.returnConnectionToPool(vivoJena, "dataSet");
		
		return inVivo;
	}
	
	/**
	 * @return A random number for an external author
	 */
	private int generateRandomNumber() {
		boolean inVivo = true;
		Random random = new Random();
		int randomNumber = random.nextInt(954321);
		String newUri = null;
		
		while(inVivo) {
			newUri = this.vivoNamespace + "person" + randomNumber;
			inVivo = isNotInVivo(newUri);
		}
		return randomNumber;
		
		
	}
	
	/**
	 * @param cwid Unique identifier for a person
	 * This function will add the inactive author as an external author and remove the link from the publication to the deleted profile
	 */
	private void addAuthorAsExternalEntity(String cwid, List<PublicationBean> publications) {
		int firstCount = 0;
		String randomNumber;
		int inferenceCount = 0;
		
		SDBJenaConnect vivoJena = null;
		//Check for person in VIVO
		/*StringBuilder sbs = new StringBuilder();
		sbs.append("SELECT ?vcard ?arg ?person \n");
		sbs.append("WHERE {\n");
		sbs.append("GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> {\n");
		sbs.append("?vcard <http://www.w3.org/2006/vcard/ns#givenName> \"" + this.givenName + "\" . \n");
		if(!this.middleName.equals(" "))
			sbs.append("OPTIONAL { ?vcard <http://vivoweb.org/ontology/core#middleName> \"" + this.middleName.trim() + "\" .} \n");
		sbs.append("?vcard <http://www.w3.org/2006/vcard/ns#familyName> \"" + this.familyName + "\" . \n");
		sbs.append("?arg <http://www.w3.org/2006/vcard/ns#hasName> ?vcard . \n");
		sbs.append("?person <http://purl.obolibrary.org/obo/ARG_2000028> ?arg . \n");
		sbs.append("FILTER(REGEX(STR(?person),\"" + this.vivoNamespace + "person\",\"i\")) \n");
		sbs.append("}}");
		vivoJena = this.jcf.getConnectionfromPool("dataSet");
		
		ResultSet rs;
		try {
			rs = vivoJena.executeSelectQuery(sbs.toString(), true);
			if(rs.hasNext()) { 
				while(rs.hasNext())
				{
					QuerySolution qs =rs.nextSolution();
					
					randomNumber = Integer.parseInt(qs.get("person").toString().replace(this.vivoNamespace + "person", ""));
					
					//logger.info("Vcard - " + qs.get("vcard").toString() + " - Triple : " + "http://vivo.med.cornell.edu/individual/cwid-" + cwid.trim() + " " + qs.get("arg").toString() + " " + qs.get("person").toString());
				}
				logger.info("External Person Found: person" + randomNumber);
			}
			else
				logger.info("No external person record found for " + cwid);
			} catch(IOException e) {
				// TODO Auto-generated catch block
				logger.info("IOException" , e);
			}
		
		this.jcf.returnConnectionToPool(vivoJena, "dataSet");
		
		//If the person does not have a record in VIVO
		if(randomNumber == 0) {
			randomNumber = getAuthorshipPk(cwid);
		}*/

		randomNumber = getExternalPersonIdentifier(this.givenName, this.familyName);		 



		Iterator<PublicationBean> i = publications.iterator();
		logger.info("Check for Additional WCMC Authored pubs.");
		while(i.hasNext())
		{
			PublicationBean pub = i.next();
			if(pub.isAdditionalWcmcAuthorFlag()) {
				/*if(firstCount==0 && randomNumber==0) {
					randomNumber = generateRandomNumber();
					firstCount = firstCount + 1;
				}*/
				StringBuilder sb = new StringBuilder();
				sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n"); 
				sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
				sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
				sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"); 
				sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
				sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n"); 
				sb.append("DELETE { \n");
				sb.append("<" + pub.getAuthorshipUrl().trim() + "> core:relates ?person . \n");
				sb.append("?person core:relatedBy <" + pub.getAuthorshipUrl().trim() + "> . \n");
				sb.append("} \n");
				sb.append("INSERT { \n");
				sb.append("<" + pub.getAuthorshipUrl().trim() + "> core:relates <" + JenaConnectionFactory.nameSpace + "person" + randomNumber +"> .\n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "person" + randomNumber +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000001> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "person" + randomNumber +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000002> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "person" + randomNumber +"> rdf:type <http://purl.obolibrary.org/obo/IAO_0000030> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "person" + randomNumber +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000031> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "person" + randomNumber +"> rdf:type <http://purl.obolibrary.org/obo/ARG_2000379> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "person" + randomNumber +"> rdf:type <http://www.w3.org/2006/vcard/ns#Kind> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "person" + randomNumber +"> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "person" + randomNumber +"> rdf:type <http://www.w3.org/2006/vcard/ns#Individual> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "person" + randomNumber +"> vitro:mostSpecificType <http://www.w3.org/2006/vcard/ns#Individual> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "person" + randomNumber +"> core:relatedBy <" + pub.getAuthorshipUrl().trim() + "> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "person" + randomNumber +"> <http://www.w3.org/2006/vcard/ns#hasName> <" + JenaConnectionFactory.nameSpace + "hasName-person" + randomNumber + "> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + randomNumber + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Explanatory> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + randomNumber + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Addressing> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + randomNumber + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Thing> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + randomNumber + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Communication> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + randomNumber + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Identification> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + randomNumber + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2006/vcard/ns#Name> . \n");
				sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + randomNumber + "> vitro:mostSpecificType <http://www.w3.org/2006/vcard/ns#Name> . \n");
				if(this.givenName != null)
					sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + randomNumber + "> <http://www.w3.org/2006/vcard/ns#givenName> \"" + this.givenName.replaceAll("'", "\'") + "\" . \n");
				if(this.familyName != null)
					sb.append("<" + JenaConnectionFactory.nameSpace + "hasName-person" + randomNumber + "> <http://www.w3.org/2006/vcard/ns#familyName> \"" + this.familyName.replaceAll("'", "\'") + "\" . \n");
				sb.append("}\n");
				sb.append("WHERE { \n");
				sb.append("OPTIONAL { <" + pub.getAuthorshipUrl().trim() + "> core:relates ?person .\n");
				sb.append("?person core:relatedBy <" + pub.getAuthorshipUrl().trim() + "> .\n");
				sb.append("FILTER(REGEX(STR(?person),\"cwid\",\"i\") || REGEX(STR(?person),\"person\",\"i\")) }\n");
				sb.append("}");
				
				logger.info(sb.toString());
				
				
				if(ingestType.equals(IngestType.VIVO_API.toString())) {
					logger.info(this.vivoClient.vivoUpdateApi(sb.toString()));
				} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
					vivoJena = this.jcf.getConnectionfromPool("dataSet");
					
					try {
						vivoJena.executeUpdateQuery(sb.toString(), true);
		
					} catch(IOException e) {
						// TODO Auto-generated catch block
						logger.error("IOException" , e);
					}
					this.jcf.returnConnectionToPool(vivoJena, "dataSet");
					//Insert into inference graph
					
					if(inferenceCount == 0) {
						logger.info("Insert into inference graph");
						String sparqlQuery = "PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n" +
							"INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> { \n" +
							"<" + this.vivoNamespace + "person" + randomNumber +"> <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#mostSpecificType> wcmc:ExternalEntity . \n" +
							"<" + pub.getAuthorshipUrl().trim() + "> <http://purl.obolibrary.org/obo/ARG_2000028> <" + this.vivoNamespace + "arg2000028-" + randomNumber +"> .\n" +
							"}}";
						if(ingestType.equals(IngestType.VIVO_API.toString())) {
							logger.info(this.vivoClient.vivoUpdateApi(sparqlQuery));
						} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
							vivoJena = this.jcf.getConnectionfromPool("dataSet");
							try {
								vivoJena.executeUpdateQuery(sparqlQuery, true);
							} catch(IOException e) {
								// TODO Auto-generated catch block
								logger.error("IOException" , e);
							}
							this.jcf.returnConnectionToPool(vivoJena, "dataSet");
						}
					}
				}
				inferenceCount = inferenceCount + 1;
			}	
		}
	}
	
	/**
	 * This function returns the lowest authorship pk from Pubadmin
	 * @param cwid Unique identifier in WCMC
	 * @return authorshipPk
	 */
	private int getAuthorshipPk(String cwid) {
		
		int authorshipPk = 0;
		String selectQuery = "select min(wcmc_authorship_pk) from wcmc_authorship where cwid = '" + cwid.trim() + "'";
		
		PreparedStatement ps = null;
		java.sql.ResultSet rs = null;
		try {
				ps = this.con.prepareStatement(selectQuery);
				rs = ps.executeQuery();
				rs.next();
				authorshipPk = rs.getInt(1);
		}
		catch(SQLException e) {
			logger.error("SQLException" , e);
		}
		finally {
			try{
				if(ps!=null)
					ps.close();
				if(rs!=null)
					rs.close();
			}
			catch(Exception e) {
				logger.error("Exception",e);
			}
			
		}
		return authorshipPk;
	}
	/**
	 * This function deletes any remaining triples related to the inactive profile. This will ensure complete deletion of the profile.
	 * @param cwid Unique identifier for institution
	 */
	private void deleteRemainingTriples(String cwid) {
		
		
		StringBuilder sb = new StringBuilder();
		
		final class Triples {
			private String graphName;
			private String subject;
			private String predicate;
			private String object;
			
			private Triples(String graphName, String subject, String predicate, String object) {
				this.graphName = graphName;
				this.subject = subject;
				this.predicate = predicate;
				this.object = object;
			}
		}
		
		List<Triples> triples = new ArrayList<Triples>();
		
		sb.append("SELECT ?g ?p ?o \n");
		sb.append("WHERE { \n");
		sb.append("GRAPH ?g { \n");
		sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> ?p ?o . \n");
		sb.append("}}");
		
		//logger.info(sb.toString());
		logger.info("Fetching all the remaining triples from different graphs for cleanup for " + cwid);
		if(ingestType.equals(IngestType.VIVO_API.toString())) {
			String response = vivoClient.vivoQueryApi(sb.toString());
			logger.info(response);
			JSONObject obj = new JSONObject(response);
			JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
			if(bindings != null && !bindings.isEmpty()) {
				for (int i = 0; i < bindings.length(); ++i) {
					if(bindings.getJSONObject(i).optJSONObject("g") != null && bindings.getJSONObject(i).optJSONObject("g").has("value")
					&&
					bindings.getJSONObject(i).optJSONObject("p") != null && bindings.getJSONObject(i).optJSONObject("p").has("value")
					&&
					bindings.getJSONObject(i).optJSONObject("o") != null && bindings.getJSONObject(i).optJSONObject("o").has("value")) {
						triples.add(new Triples(bindings.getJSONObject(i).getJSONObject("g").getString("value"), this.vivoNamespace + "cwid-" + cwid.trim(), bindings.getJSONObject(i).getJSONObject("p").getString("value"), bindings.getJSONObject(i).getJSONObject("o").getString("value")));
						logger.info("Graph - " + bindings.getJSONObject(i).getJSONObject("g").getString("value") + " - Triple: " + this.vivoNamespace + "cwid-" + cwid.trim()+ " " + bindings.getJSONObject(i).getJSONObject("p").getString("value") + " " + bindings.getJSONObject(i).getJSONObject("o").getString("value"));
					}
				}
			}
		} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
			SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
			ResultSet rs;
			try {
				rs = vivoJena.executeSelectQuery(sb.toString(), true);
				while(rs.hasNext())
				{
					QuerySolution qs =rs.nextSolution();
					
					
					if(qs.get("g")!=null && qs.get("p") !=null && qs.get("o") != null) {
						
						triples.add(new Triples(qs.get("g").toString(), this.vivoNamespace + "cwid-" + cwid.trim() , qs.get("p").toString(), qs.get("o").toString()));
					
						logger.info("Graph - " + qs.get("g").toString() + " - Triple : " + this.vivoNamespace + "cwid-" + cwid.trim() + " " + qs.get("p").toString() + " " + qs.get("o").toString());
					}
				}
				} catch(IOException e) {
					// TODO Auto-generated catch block
					logger.info("IOException" , e);
				}
				
			this.jcf.returnConnectionToPool(vivoJena, "dataSet");
		}
		
		sb.setLength(0);
		
		
		//Delete Query
		if(!triples.isEmpty()) {
			sb.append("DELETE DATA { \n");
			for(Triples t : triples) {
				if(!t.object.trim().contains("http://")) {
					sb.append("GRAPH <" + t.graphName.trim() + "> { \n");
					sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> <" + t.predicate.trim() + "> \"" + t.object.trim() + "\" . \n");
					sb.append("} \n");
				}
				else {
					sb.append("GRAPH <" + t.graphName.trim() + "> { \n");
					sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> <" + t.predicate.trim() + "> <" + t.object.trim() + "> . \n");
					sb.append("} \n");
				}
			}
			sb.append("} \n");
			/*sb.append("WHERE { \n");
			for(Triples t : triples) {
				if(!t.object.trim().contains("http://"))
					sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> <" + t.predicate.trim() + "> \"" + t.object.trim() + "\" . \n");
				else
					sb.append("<" + this.vivoNamespace + "cwid-" + cwid.trim() + "> <" + t.predicate.trim() + "> <" + t.object.trim() + "> . \n");
			}
			sb.append("}");*/
			
			logger.info(sb.toString());
			if(ingestType.equals(IngestType.VIVO_API.toString())) {
				logger.info(this.vivoClient.vivoUpdateApi(sb.toString()));
			} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())){
				SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
				
				logger.info("Deleting all the remaining triples for cwid - " + cwid );
				try {
					vivoJena.executeUpdateQuery(sb.toString(), true);
				} catch(IOException e) {
					logger.error("Error connecting to SDBJena");
				}
				
				this.jcf.returnConnectionToPool(vivoJena, "dataSet");
			}
			
			
		}
		else
			logger.info("No additional triples needs to be deleted for " + cwid );
		
		
	}
	
	
	
	/**
	 * Execute method to perform all the steps needed for successful deletion
	 */
	public void execute() {
		
		int inActiveCount = 0;
		int activeCount = 0;
		List<String> people = edi.getPeopleInVivo();
		if(people.isEmpty())
			logger.info("No People needs to be deleted");
		
		logger.info("Checking in all other graph");
		
		String sparqlQuery = "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
			 "PREFIX foaf:<http://xmlns.com/foaf/0.1/> \n" +
			 "SELECT  distinct ?people \n" +
			 "WHERE \n" +
			 "{ GRAPH ?g {\n" +
			 "?people rdf:type foaf:Person . \n" +
			 "FILTER(REGEX(STR(?people),\"" + this.vivoNamespace + "cwid-\")) \n" +
			 //"MINUS { \n" +
			 //"graph <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> {?people rdf:type foaf:Person .} \n" +
			 //"}\n" +
			 "}}";
		
		//logger.info(sparqlQuery);
		if(ingestType.equals(IngestType.VIVO_API.toString())) {
			try {
				String response = this.vivoClient.vivoQueryApi(sparqlQuery);
				logger.info(response);
				JSONObject obj = new JSONObject(response);
				JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
				if(bindings != null && !bindings.isEmpty()) {
					for (int i = 0; i < bindings.length(); ++i) {
						if(bindings.getJSONObject(i).optJSONObject("people") != null && bindings.getJSONObject(i).optJSONObject("people").has("value")) {
							people.add(bindings.getJSONObject(i).getJSONObject("people").getString("value").replace(this.vivoNamespace + "cwid-", "").trim());
						}
					}
				}
				
			} catch(Exception e) {
				logger.error("Api Exception", e);
			}
		} else {
		
			SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
			ResultSet rs;
			try {
				rs = vivoJena.executeSelectQuery(sparqlQuery,true);
			
			
			while(rs.hasNext())
			{
				QuerySolution qs =rs.nextSolution();

				if(qs.get("people") != null && !people.contains(qs.get("people").toString().replace(this.vivoNamespace + "cwid-", "").trim())) {
					people.add(qs.get("people").toString().replace(this.vivoNamespace + "cwid-", "").trim());
				}
				
			}
			} catch(IOException e) {
				// TODO Auto-generated catch block
				logger.info("IOException" , e);
			}
			this.jcf.returnConnectionToPool(vivoJena, "dataSet");
		}
			
		
			
		
		Iterator<String> it = people.iterator();
		while(it.hasNext()) {
			String cwid = it.next().trim();
			if(!checkForInActivePeopleEd(cwid)) {
				List<PublicationBean> publications = new ArrayList<PublicationBean>();
				Map<String, String> grants = new HashMap<String, String>();
				logger.info("###########################################");
				logger.info("Cwid - " + cwid + " needs to be deleted");
				logger.info("Getting list of publications for " + cwid);
				getListofPublications(cwid, publications);
				logger.info("Getting list of grants for " + cwid);
				getListOfGrants(cwid, grants);
				if(publications.isEmpty()) {
					try {
						deleteProfile(cwid, publications, grants);
						logger.info("Pubs is empty");
					} catch(IOException e) {
						logger.info("IOException", e);
					}
				}
				else {
					 	checkAdditionalWCMCAuthoredPubs(publications);
					 	for(PublicationBean pb : publications) {
							logger.info(pb.toString());
						}
					try {
						deleteProfile(cwid, publications, grants);
					} catch(IOException e) {
						// TODO Auto-generated catch block
						logger.info("IOException", e);
					}
					addAuthorAsExternalEntity(cwid, publications);
				}
				deleteRemainingTriples(cwid);
				logger.info("###########################################");
				
				inActiveCount = inActiveCount + 1;
			}
			else {
				logger.info("Cwid: " + cwid + " does not have to be deleted");
				activeCount = activeCount + 1;
			}
		}
		logger.info("Total inactive profile deleted: " + inActiveCount);
		logger.info("Total active profiles in VIVO: " + activeCount);
		
	}

	private String getExternalPersonIdentifier(String firstname, String lastname) {
        return DigestUtils.md5Hex(firstname + lastname).toLowerCase();

    }
	
	/**
	 * @param args
	 * This is the main method
	 */
	public static void main(String args[]) {
		if (args.length == 0) {
			logger.info("Usage: java fetch.JSONPeopleFetch [properties filename]");
			logger.info("e.g. java fetch.JSONPeopleFetch /usr/share/vivo-ed-people/examples/wcmc_people.properties");
		} else if (args.length == 1) { // path of the properties file
			propertyFilePath = args[0];
			
			try {
				props.load(new FileInputStream(propertyFilePath));
			} catch (FileNotFoundException e) {
				logger.info("File not found error: " + e);
			} catch (IOException e) {
				logger.info("IOException error: " + e);
			}
			
			ldapbasedn = props.getProperty("ldapBaseDN");
			
			new DeleteProfile().execute();
		}
	}
	
	
}

