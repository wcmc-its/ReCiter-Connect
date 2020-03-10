package org.vivoweb.harvester.ingest;

import java.util.List;

import reciter.connect.database.mysql.jena.JenaConnectionFactory;
/**
 * @author Sarbajit Dutta (szd2013@med.cornell.edu)
 * <p><b><i>This interface has functions which checks data from VIVO and Enterprise Directory<p><b><i>
 */
public interface EdDataInterface {
	
	/**
	 * @param propertyFilePath The path to the property file
	 * @param jcf Jena connection factory object
	 * @return List of people in VIVO
	 */
	public List<String> getPeopleInVivo(JenaConnectionFactory jcf);
}
