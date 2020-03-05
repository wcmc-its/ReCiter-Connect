package org.vivoweb.harvester.connectionfactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;

/**
 * @author Sarbajit Dutta (szd2013@med.cornell.edu)
 * <p><b><i>This class creates and manages connections for Jena related connections. You can create , get , return and remove connection from pool with different
 * functional methods.<p><b><i>
 */
public class JenaConnectionFactory {
	
	private String propertyFilePath = null;
	private Properties props = new Properties();
	private static Logger log = LoggerFactory.getLogger(JenaConnectionFactory.class);
	
	/**
	 * This is the default namespace for your institution
	 */
	public static String nameSpace;
	
	String dbHost = null; 
	String jenaDbUser = null;
	String jenaDbPassword = null;
	String dbType = null;
	String dbDriver = null;
	String dbLayout = null;
	String dbModel = null; 
	
	private Map<SDBJenaConnect, String> connectionPool = new HashMap<SDBJenaConnect, String>(); 
	
	private static JenaConnectionFactory instance = null;
	
	/**
	 * @param propertyFilePath the path of property file
	 */
	public JenaConnectionFactory(String propertyFilePath) {
		this.propertyFilePath = propertyFilePath;
		initialize();
	}
	
	/**
	 * @param propertyFilePath the path of property file
	 * @return an instance of LDAP connection
	 * This method returns a singleton object
	 */
	public static JenaConnectionFactory getInstance(String propertyFilePath) {
		if(instance == null)
			instance = new JenaConnectionFactory(propertyFilePath);
		
		return instance;
	}
	
	/**
	 * This method initializes and creates and populates the connection pool
	 */
	private void initialize() {
		initializeConnectionPool();
		
	}
	
	/**
	 * This method initializes and creates and populates the connection pool
	 */
	private void initializeConnectionPool() {
		while(!checkIfConnectionPoolIsFull()) {
			log.info("Jena pool is not full. Proceeding with adding new connection");
			this.connectionPool.put(createNewConnectionForPool(this.propertyFilePath, "http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople"),"wcmcPeople");
			this.connectionPool.put(createNewConnectionForPool(this.propertyFilePath, "http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa"),"wcmcOfa");
			this.connectionPool.put(createNewConnectionForPool(this.propertyFilePath, "http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus"),"wcmcCoeus");
			this.connectionPool.put(createNewConnectionForPool(this.propertyFilePath, "http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications"),"wcmcPublications");
			this.connectionPool.put(createNewConnectionForPool(this.propertyFilePath, "http://vitro.mannlib.cornell.edu/a/graph/wcmcVivo"),"wcmcVivo");
			this.connectionPool.put(createNewConnectionForPool(this.propertyFilePath, "http://vitro.mannlib.cornell.edu/default/vitro-kb-inf"),"vitro-kb-inf");
			this.connectionPool.put(createNewConnectionForPool(this.propertyFilePath, "http://vitro.mannlib.cornell.edu/default/vitro-kb-2"),"vitro-kb-2");
			this.connectionPool.put(createNewDataSetConnectionForPool(this.propertyFilePath),"dataSet");
		}
		log.info("Jena connection pool is full");
	}
	
	/**
	 * This method checks if connection Pool is full or not
	 * @return boolean
	 */
	private synchronized boolean checkIfConnectionPoolIsFull() {
		final int MAX_POOL_SIZE = 8;
		if(this.connectionPool.size()<MAX_POOL_SIZE)
			return false;
		else
			return true;
	}
	
	/**
	 * The function gets a connection from pool for a specified graph name
	 * @param graphName the graphName for which the connection will be created
	 * @return the jena connection object
	 */
	public synchronized SDBJenaConnect getConnectionfromPool(String graphName){
			
		SDBJenaConnect con = null;
			if(this.connectionPool.size()>0){
				Iterator<Entry<SDBJenaConnect, String>> it = this.connectionPool.entrySet().iterator();
				while(it.hasNext()) {
					Entry<SDBJenaConnect, String> pair = it.next();
					if(graphName.equals(pair.getValue())) {
						con = pair.getKey();
						
					}
					
				}
				this.connectionPool.remove(con);
				
			}
			return con;
	}
	
	/**
	 * This fuctions returns a connection for a graph Name specified
	 * @param connection The connection object to SDBJena
	 * @param graphName the graphName for which the connection will be created
	 */
	public synchronized void returnConnectionToPool(SDBJenaConnect connection, String graphName)
    {
        //Adding the connection from the client back to the connection pool
        this.connectionPool.put(connection, graphName);
    }
	
	/**
	 * This function iterate through the connection pool and destroys all the connections
	 */
	public void destroyConnectionPool() {
		for (SDBJenaConnect key : this.connectionPool.keySet()) {
			if(key != null)
				key.close();
		}
		this.connectionPool.clear();
		log.info("All Jena connections were destroyed");
	} 
	
	/**
	 * This function will create a new connection for pool
	 * @param filePath The file path for the property file
	 * @param graphName the graphName for which the connection will be created
	 * @return the jena connection object
	 */
	public SDBJenaConnect createNewConnectionForPool(String filePath, String graphName)
	{
		
		SDBJenaConnect vivoJena = null;
		try {
			this.props.load(new FileInputStream(filePath));
		} catch (FileNotFoundException fe) {
			log.info("File not found error: " + fe);
		} catch (IOException e) {
			log.info("IOException error: " + e);
		}
		
		this.dbHost = this.props.getProperty("dbHost");
		this.dbDriver = this.props.getProperty("dbDriver");
		this.jenaDbUser = this.props.getProperty("dbUsername");
		this.jenaDbPassword = this.props.getProperty("dbPassword");
		this.dbLayout = this.props.getProperty("dbLayout");
		this.dbType=this.props.getProperty("dbType");
		this.dbModel=this.props.getProperty("dbModel");
		
		
		try {
			 vivoJena = new SDBJenaConnect(this.dbHost, this.jenaDbUser, this.jenaDbPassword, this.dbType, this.dbDriver, this.dbLayout, graphName);
		} catch(IOException e) {
			// TODO Auto-generated catch block
			log.error("IOException", e);
		}
		return vivoJena;
	
	}
	
	/**
	 * This function will create a new connection for pool without modelName which enables us to query across all models(graphs)
	 * @param filePath The file path for the property file
	 * @param graphName the graphName for which the connection will be created
	 * @return the jena connection object
	 */
	public SDBJenaConnect createNewDataSetConnectionForPool(String filePath)
	{
		
		SDBJenaConnect vivoJena = null;
		try {
			this.props.load(new FileInputStream(filePath));
		} catch (FileNotFoundException fe) {
			log.info("File not found error: " + fe);
		} catch (IOException e) {
			log.info("IOException error: " + e);
		}
		
		this.dbHost = this.props.getProperty("dbHost");
		this.dbDriver = this.props.getProperty("dbDriver");
		this.jenaDbUser = this.props.getProperty("dbUsername");
		this.jenaDbPassword = this.props.getProperty("dbPassword");
		this.dbLayout = this.props.getProperty("dbLayout");
		this.dbType=this.props.getProperty("dbType");
		if(this.props.getProperty("vivoNamespace") != null && this.props.getProperty("vivoNamespace").trim().length() != 0) {
			JenaConnectionFactory.nameSpace=(this.props.getProperty("vivoNamespace").trim().endsWith("/"))?this.props.getProperty("vivoNamespace").trim():this.props.getProperty("vivoNamespace").trim().concat("/");
		}
		
		
		try {
			 vivoJena = new SDBJenaConnect(this.dbHost, this.jenaDbUser, this.jenaDbPassword, this.dbType, this.dbDriver, this.dbLayout);
		} catch(IOException e) {
			// TODO Auto-generated catch block
			log.error("IOException", e);
		}
		return vivoJena;
	
	}
}
