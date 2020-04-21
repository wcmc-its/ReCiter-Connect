package org.vivoweb.harvester.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;

import lombok.extern.slf4j.Slf4j;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;


/**
 * @author Sarbajit Dutta (szd2013@med.cornell.edu)
 * <p><b><i>This class overrides the interface for EdDataInterface<p><b><i>
 */
@Service("edDataInterface")
@Slf4j
public class EdDataInterfaceImpl implements EdDataInterface {

	public List<String> getPeopleInVivo(JenaConnectionFactory jcf) {
		
		List<String> people = new ArrayList<String>();

		String sparqlQuery = "PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
				"PREFIX foaf:     <http://xmlns.com/foaf/0.1/> \n" +
				"SELECT  ?people \n" +
				"FROM <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> \n" +
				"WHERE \n" +
				"{ \n" +
				"?people rdf:type foaf:Person . \n" +
				//"FILTER(REGEX(STR(?people),\"rak2007\")) \n" +
				"}";
		SDBJenaConnect vivoJena = jcf.getConnectionfromPool("wcmcPeople");
		ResultSet rs;
		try {
			rs = vivoJena.executeSelectQuery(sparqlQuery);
			while(rs.hasNext())
			{
				QuerySolution qs =rs.nextSolution();
				if(qs.get("people") != null) {
					people.add(qs.get("people").toString().replace(JenaConnectionFactory.nameSpace + "cwid-", "").trim());
				}
				
			}
		} catch(IOException e) {
			log.info("IOException" , e);
		}
		jcf.returnConnectionToPool(vivoJena, "wcmcPeople");
		return people;
	}
	
}
