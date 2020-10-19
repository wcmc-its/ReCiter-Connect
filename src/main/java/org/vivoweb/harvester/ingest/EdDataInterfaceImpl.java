package org.vivoweb.harvester.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;

import lombok.extern.slf4j.Slf4j;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;
import reciter.connect.vivo.IngestType;
import reciter.connect.vivo.api.client.VivoClient;


/**
 * @author Sarbajit Dutta (szd2013@med.cornell.edu)
 * <p><b><i>This class overrides the interface for EdDataInterface<p><b><i>
 */
@Service("edDataInterface")
@Slf4j
public class EdDataInterfaceImpl implements EdDataInterface {

	@Autowired
	private VivoClient vivoClient;

	@Autowired
	private JenaConnectionFactory jcf;

	private String ingestType = System.getenv("INGEST_TYPE");

	public List<String> getPeopleInVivo(JenaConnectionFactory jcf) {
		
		List<String> people = new ArrayList<String>();

		String sparqlQuery = "PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
				"PREFIX foaf:     <http://xmlns.com/foaf/0.1/> \n" +
				"SELECT  ?people \n" +
				"WHERE {\n" +
				"GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> \n" +
				"{ \n" +
				"?people rdf:type foaf:Person . \n" +
				//"FILTER(REGEX(STR(?people),\"bhb9002\")) \n" +
				"}}";

		if(ingestType.equals(IngestType.VIVO_API.toString())) {
			try{
				String response = vivoClient.vivoQueryApi(sparqlQuery);
				log.info(response);
				JSONObject obj = new JSONObject(response);
				JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
				if(bindings != null && !bindings.isEmpty()) {
					for (int i = 0; i < bindings.length(); ++i) {
						people.add(bindings.getJSONObject(i).getJSONObject("people").getString("value").replace(JenaConnectionFactory.nameSpace + "cwid-", ""));
					}
				} else {
					log.info("No result from the query");
				}
			} catch(Exception  e) {
				log.info("Api Exception", e);
			}
		} else if(ingestType.equals(IngestType.SDB_DIRECT.toString())) {
			SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("dataSet");
			try {
				ResultSet rs = vivoJena.executeSelectQuery(sparqlQuery, true);
				while(rs.hasNext())
				{
					QuerySolution qs =rs.nextSolution();
					if(qs.get("people") != null) {
						people.add(qs.get("people").toString().replace(JenaConnectionFactory.nameSpace + "cwid-", "").trim());
					}
					
				}
			} catch(IOException e) {
				log.error("Error connecting to Jena database", e);
			}
			this.jcf.returnConnectionToPool(vivoJena, "dataSet");
		}
		return people;
	}
	
}
