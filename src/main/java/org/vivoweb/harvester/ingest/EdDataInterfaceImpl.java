package org.vivoweb.harvester.ingest;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;

import lombok.extern.slf4j.Slf4j;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;
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

		try {
			String response = vivoClient.vivoQueryApi(sparqlQuery);
			log.info(response);
			JSONObject obj = new JSONObject(response);
			JSONArray bindings = obj.getJSONObject("results").getJSONArray("bindings");
			if(bindings != null && !bindings.isEmpty()) {
				for (int i = 0; i < bindings.length(); ++i) {
					people.add(bindings.getJSONObject(i).getJSONObject("people").getString("value").replace("http://vivo.med.cornell.edu/individual/cwid-", ""));
				}
			} else {
				log.info("No result from the query");
			}
		} catch(Exception e) {
			log.error("Exception", e);
		}
		return people;
	}
	
}
