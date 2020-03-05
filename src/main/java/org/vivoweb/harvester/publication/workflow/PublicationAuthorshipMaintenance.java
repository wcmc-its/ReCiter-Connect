package org.vivoweb.harvester.publication.workflow;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vivoweb.harvester.connectionfactory.JenaConnectionFactory;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import reciter.connect.beans.vivo.*;

/**
 * @author szd2013
 * <p><b><i> This class fetches publications from PubAdmin and checks against VIVO whether they exist or not and inserts them in VIVO.
 * It will check for updates for citation count, publication type. </i></b></p>
 */
public class PublicationAuthorshipMaintenance {
	
	/**
	 * <i>This is the file path of the property file</i>
	 */
	public static String propertyFilePath = null;
	
	/**
	 * <i>This is the global connection variable for all connections to PubAdmin</i>
	 */
	private Connection con = null;
	
	/**
	 * <i>This is the logger variable for all logging</i>
	 */
	private static Logger log = LoggerFactory.getLogger(PublicationAuthorshipMaintenance.class);
	
	

	

	/**
	 * <p><b><i>This method fetches all the valid publications and their relevant fields from PubAdmin and stores them in a PublicationBean type Set <p><b><i>
	 * @param cwid <i> This is the center wide unique identifier</i>
	 * @param pa PublicationAuthprship Bean containing all information about a publications
	 * @param ab AuthorBean containing Author information
	 * @param jcf The connection factory object for all jena sdb connections
	 * @param sb The StringBuilder object from PublicationFetch class
	*/
	public static void fixAuthorshipInVivo(String cwid, PublicationAuthorshipMaintenanceBean pa, AuthorBean ab, JenaConnectionFactory jcf, StringBuilder sb, String vivoNamespace) {

		String personId = pa.getAuthorUrl().trim().replace(vivoNamespace + "person", "");
		
		sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
		sb.append("DELETE { \n");
		sb.append("<" + pa.getAuthorshipUrl().trim() + "> <http://vivoweb.org/ontology/core#relates> <" + pa.getAuthorUrl() + "> . \n");
		sb.append("<" + pa.getAuthorshipUrl().trim() + "> <http://vivoweb.org/ontology/core#relates> <" + vivoNamespace + "arg2000028-" + personId.trim() + "> . \n");
		sb.append("<" + pa.getAuthorUrl().trim() + "> ?p ?o . \n");
		sb.append("<" + vivoNamespace + "arg2000028-" + personId.trim() + "> ?p1 ?o1 . \n");
		sb.append("<" + vivoNamespace + "hasName-person" + personId.trim() + "> ?p2 ?o2 . \n");
		sb.append("<" + vivoNamespace + "hasTitle-person" + personId.trim() + "> ?p3 ?o3 . \n");
		sb.append("} \n");
		sb.append("INSERT { \n");
		sb.append("<" + pa.getAuthorshipUrl().trim() + "> <http://vivoweb.org/ontology/core#relates> <" + vivoNamespace + "cwid-" + cwid.trim() + "> . \n");
		sb.append("<" + pa.getAuthorshipUrl().trim() + "> <http://vivoweb.org/ontology/core#relates> <" + vivoNamespace + "arg2000028-" + cwid.trim() + "> . \n");
		sb.append("<" + vivoNamespace + "cwid-" + cwid.trim() + "> <http://vivoweb.org/ontology/core#relatedBy> <" + pa.getAuthorshipUrl().trim() + "> . \n");
		sb.append("<" + vivoNamespace + "arg2000028-" + cwid.trim() + "> <http://purl.obolibrary.org/obo/ARG_2000029> <" + pa.getAuthorshipUrl().trim() + "> . \n");
		sb.append("<" + vivoNamespace + "arg2000028-" + cwid.trim() + "> <http://vivoweb.org/ontology/core#relatedBy> <" + pa.getAuthorshipUrl().trim() + "> . \n");
		sb.append("} \n");
		sb.append("WHERE {\n");
		sb.append("<" + pa.getAuthorshipUrl().trim() + "> <http://vivoweb.org/ontology/core#relates> <" + pa.getAuthorUrl() + "> . \n");
		sb.append("OPTIONAL { <" + pa.getAuthorshipUrl().trim() + "> <http://vivoweb.org/ontology/core#relates> <" + vivoNamespace + "arg2000028-" + personId.trim() + "> .} \n");
		sb.append("OPTIONAL { <" + pa.getAuthorUrl().trim() + "> ?p ?o . } \n");
		sb.append("OPTIONAL { <" + vivoNamespace + "arg2000028-" + personId.trim() + "> ?p1 ?o1 .} \n");
		sb.append("OPTIONAL { <" + vivoNamespace + "hasName-person" + personId.trim() + "> ?p2 ?o2 .} \n");
		sb.append("OPTIONAL { <" + vivoNamespace + "hasTitle-person" + personId.trim() + "> ?p3 ?o3 .} \n");
		sb.append("}");
		
		log.info(sb.toString());
		
		log.info("Deleting authorship: " + pa.getAuthorshipUrl() + "\nfor publication: " + pa.getPubUrl() + " having author as " + pa.getAuthorUrl() + " \nand assigning correct author " + vivoNamespace + "cwid-" + ab.getCwid() );
		
		SDBJenaConnect vivoJena = jcf.getConnectionfromPool("wcmcPublications");
		try {
			vivoJena.executeUpdateQuery(sb.toString(), true);
		} catch(IOException e) {
			log.error("Error connecting to SDBJena");
		}
		jcf.returnConnectionToPool(vivoJena, "wcmcPublications");
		
		//Deal with inference graph for person
		if(sb.length()>0)
			sb.setLength(0);
		
		sb.append("WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> \n");
		sb.append("DELETE { \n");
		sb.append("<" + pa.getAuthorUrl().trim() + "> ?p ?o . \n");
		sb.append("<" + pa.getAuthorshipUrl().trim() + "> <http://purl.obolibrary.org/obo/ARG_2000028> <" + vivoNamespace + "arg2000028-" + personId.trim() + "> . \n");
		sb.append("} \n");
		sb.append("INSERT {\n");
		sb.append("<" + pa.getAuthorshipUrl().trim() + "> <http://purl.obolibrary.org/obo/ARG_2000028> <" + vivoNamespace + "arg2000028-" + cwid.trim() + "> . \n");
		sb.append("} \n");
		sb.append("WHERE {\n");
		sb.append("OPTIONAL {<" + pa.getAuthorUrl().trim() + "> ?p ?o . } \n");
		sb.append("OPTIONAL {<" + pa.getAuthorshipUrl().trim() + "> <http://purl.obolibrary.org/obo/ARG_2000028> <" + vivoNamespace + "arg2000028-" + personId.trim() + "> . } \n");
		sb.append("}");
		
		vivoJena = jcf.getConnectionfromPool("vitro-kb-inf");
		
		try {
			vivoJena.executeUpdateQuery(sb.toString(), true);
		} catch(IOException e) {
			log.error("Error connecting to SDBJena");
		}
		log.info("Deleting triples in inference graph for " + pa.getAuthorUrl());
		jcf.returnConnectionToPool(vivoJena, "vitro-kb-inf");
	}
	
	/**
	 * @param pub Publication URI 
	 * @param jcf The connection factory object for all jena sdb connections
	 * @param sb The StringBuilder object from fetch class
	 * @return List of publications authorships that are in VIVO for a publication
	 */
	public static List<PublicationAuthorshipMaintenanceBean> getAuthorshipsFromVivo(String pub, List<PublicationAuthorshipMaintenanceBean> publication, JenaConnectionFactory jcf, StringBuilder sb, String vivoNamespace) {
		
		//List<PublicationAuthorshipMaintenanceBean> publication = new ArrayList<PublicationAuthorshipMaintenanceBean>();
		
		sb.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n"); 
		sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"); 
		sb.append("SELECT ?authorship ?Author \n");
		sb.append("from <http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications> \n");
		sb.append("WHERE { \n");
		sb.append("<" + pub + "> vivo:relatedBy ?authorship . \n");
		sb.append("?authorship vivo:relates ?Author . \n");
		sb.append("FILTER(REGEX(STR(?Author),\"cwid\",\"i\") || REGEX(STR(?Author),\"person\",\"i\") && !REGEX(STR(?Author),\"arg2000028\",\"i\")) \n");
		sb.append("}");
		
		log.info("Fetching all the authorship details for publication: " + pub + " in VIVO");
		
		SDBJenaConnect vivoJena = jcf.getConnectionfromPool("wcmcPublications");
		ResultSet rs;
		try {
			rs = vivoJena.executeSelectQuery(sb.toString());
			while(rs.hasNext())
			{
				QuerySolution qs =rs.nextSolution();
				PublicationAuthorshipMaintenanceBean pb = new PublicationAuthorshipMaintenanceBean();
				if(qs.get("Author") != null) {
					pb.setAuthorUrl(qs.get("Author").toString());
				}
				if(qs.get("authorship") != null) {
					pb.setAuthorshipUrl(qs.get("authorship").toString());
				}
				pb.setPubUrl(pub);
				
				publication.add(pb);
				
			}
			} catch(IOException e) {
				// TODO Auto-generated catch block
				log.info("IOException" , e);
			}
			
		jcf.returnConnectionToPool(vivoJena, "wcmcPublications");
		
			return publication;
	}
	
	
}

