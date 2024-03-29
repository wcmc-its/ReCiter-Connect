package reciter.connect.database.mysql.jena;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.sql.DriverManager;

import javax.inject.Inject;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;

import lombok.extern.slf4j.Slf4j;
import reciter.connect.vivo.sdb.VivoGraphs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;


/**
 * @author Sarbajit Dutta (szd2013@med.cornell.edu)
 * <p><b><i>This class creates and manages connections for Jena related connections. You can create , get , return and remove connection from pool with different
 * functional methods.<p><b><i>
 */
@Slf4j
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class JenaConnectionFactory {
	
	/**
	 * This is the default namespace for your institution
	 */
    public String nameSpaceProp;
    public static String nameSpace;
	
	private String dbHost = null; 
	private String jenaDbUser = null;
	private String jenaDbPassword = null;
	private String dbType = null;
	private String dbDriver = null;
	private String dbLayout = null;
	private String dbModel = null; 
	
	private Map<SDBJenaConnect, String> connectionPool = new HashMap<SDBJenaConnect, String>(); 
	
	/**
	 * @param propertyFilePath the path of property file
	 */
    @Inject
    @Autowired(required = true)
    public JenaConnectionFactory(@Value("${jena.dbUsername}") String username, Environment env, @Value("${jena.url}") String url,
    @Value("${jena.dbModel}") String dbModel, @Value("${jena.dbLayout}") String dbLayout, @Value("${jena.dbType}") String dbType,
    @Value("${jena.dbDriver}") String dbDriver, @Value("${vivoNamespace}") String namespace) {
        this.jenaDbUser = username;
        this.jenaDbPassword = env.getProperty("JENA_DB_PASSWORD");
        this.dbHost = url;
        this.dbType = dbType;
        this.dbModel = dbModel;
        this.dbLayout = dbLayout;
		this.dbDriver = dbDriver;
		this.nameSpaceProp = namespace;
		//initialize();
		if(nameSpaceProp != null && nameSpaceProp.trim().length() != 0) {
			JenaConnectionFactory.nameSpace=(nameSpaceProp.trim().endsWith("/"))?nameSpaceProp.trim():nameSpaceProp.trim().concat("/");
        }
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
			this.connectionPool.put(createNewDataSetConnectionForPool(),"dataSet");
		}
		log.info("Jena connection pool is full");
	}
	
	/**
	 * This method checks if connection Pool is full or not
	 * @return boolean
	 */
	private synchronized boolean checkIfConnectionPoolIsFull() {
		final int MAX_POOL_SIZE = 25;
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
	public SDBJenaConnect createNewConnectionForPool(String graphName)
	{
		
		SDBJenaConnect vivoJena = null;		
		try {
            vivoJena = new SDBJenaConnect(this.dbHost, this.jenaDbUser, this.jenaDbPassword, this.dbType, this.dbDriver, this.dbLayout, graphName);
		} catch(IOException e) {
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
	public SDBJenaConnect createNewDataSetConnectionForPool()
	{
		SDBJenaConnect vivoJena = null;
		try {
            vivoJena = new SDBJenaConnect(this.dbHost, this.jenaDbUser, this.jenaDbPassword, this.dbType, this.dbDriver, this.dbLayout);
		} catch(IOException e) {
			log.error("IOException", e);
		}
		return vivoJena;
	
	}

	public Connection getDirectConnectionToVivoDatabase()
	{
		
		Connection con = null;
		
		if (!this.dbHost.isEmpty() && !this.jenaDbUser.isEmpty() && !this.jenaDbPassword.isEmpty()) {

					try {
						con = DriverManager.getConnection(this.dbHost, this.jenaDbUser, this.jenaDbPassword);
					} 
					catch(SQLException e) {
						log.error("SQLException: " , e);
					} 
		}
		return con;
	}
}
