package reciter.connect.database.tdb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.vivoweb.harvester.util.repo.TDBJenaConnect;

import lombok.extern.slf4j.Slf4j;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;


/**
 * @author Sarbajit Dutta (szd2013@med.cornell.edu)
 * <p><b><i>This class creates and manages connections for Jena related connections. You can create , get , return and remove connection from pool with different
 * functional methods.<p><b><i>
 */
@Slf4j
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class TDBConnectionFactory {
	
	/**
	 * This is the default namespace for your institution
	 */
    public String nameSpaceProp;
    public static String nameSpace;
	
	private String dbDir = null;
	
	private Map<TDBJenaConnect, String> connectionPool = new HashMap<>(); 
	
	/**
	 * @param propertyFilePath the path of property file
	 */
    @Inject
    @Autowired(required = true)
    public TDBConnectionFactory(Environment env, @Value("${vivoNamespace}") String namespace) {
        this.dbDir = env.getProperty("TDB_DIR");
		this.nameSpaceProp = namespace;
		//initialize();
		if(nameSpaceProp != null && nameSpaceProp.trim().length() != 0) {
			TDBConnectionFactory.nameSpace=(nameSpaceProp.trim().endsWith("/"))?nameSpaceProp.trim():nameSpaceProp.trim().concat("/");
        }
	}
	
	/**
	 * This method initializes and creates and populates the connection pool
	 */
	public void initialize() {
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
		final int MAX_POOL_SIZE = 5;
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
	public synchronized TDBJenaConnect getConnectionfromPool(String graphName){
			
		TDBJenaConnect con = null;
			if(this.connectionPool.size()>0){
				Iterator<Entry<TDBJenaConnect, String>> it = this.connectionPool.entrySet().iterator();
				while(it.hasNext()) {
					Entry<TDBJenaConnect, String> pair = it.next();
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
	public synchronized void returnConnectionToPool(TDBJenaConnect connection, String graphName)
    {
        //Adding the connection from the client back to the connection pool
        this.connectionPool.put(connection, graphName);
    }
	
	/**
	 * This function iterate through the connection pool and destroys all the connections
	 */
	public void destroyConnectionPool() {
		for (TDBJenaConnect key : this.connectionPool.keySet()) {
			if(key != null)
				key.getDataset().close();
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
	public TDBJenaConnect createNewConnectionForPool(String graphName)
	{
		TDBJenaConnect vivoJena = new TDBJenaConnect(this.dbDir, graphName);		
		return vivoJena;
	}
	
	/**
	 * This function will create a new connection for pool without modelName which enables us to query across all models(graphs)
	 * @param filePath The file path for the property file
	 * @param graphName the graphName for which the connection will be created
	 * @return the jena connection object
	 */
	public TDBJenaConnect createNewDataSetConnectionForPool()
	{
		TDBJenaConnect vivoJena = new TDBJenaConnect(this.dbDir); 
		return vivoJena;
	}
}
