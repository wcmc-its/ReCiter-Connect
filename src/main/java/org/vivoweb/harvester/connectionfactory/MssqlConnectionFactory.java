package org.vivoweb.harvester.connectionfactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Sarbajit Dutta (szd2013@med.cornell.edu)
 * <p><b><i>This class creates and manages connections for mysql related connections. You can create , get , return and remove connection from pool with different
 * functional methods.<p><b><i>
 */
public class MssqlConnectionFactory {
	
	private Properties props = new Properties();
	private static Logger log = LoggerFactory.getLogger(RDBMSConnectionFactory.class);
	
	private String url = null;
	private String username = null;
	private String password = null;
	
	private Vector<Connection> connectionPool = new Vector<Connection>(); 
	
	private static MssqlConnectionFactory instance = null;
	
	private String propertyFilePath = null;
	
	private MssqlConnectionFactory() {
		
	}	
	
	/**
	 * @param propertyFilePath the path of property file
	 */
	public MssqlConnectionFactory(String propertyFilePath) {
		this.propertyFilePath = propertyFilePath;
		initialize();
	}
	
	/**
	 * @param propertyFilePath the path of property file
	 * @return an instance of LDAP connection
	 * This method returns a singleton object
	 */
	public static MssqlConnectionFactory getInstance(String propertyFilePath) {
		if(instance == null)
			instance = new MssqlConnectionFactory(propertyFilePath);
		
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
			log.info("MsSQLConnection pool is not full. Proceeding with adding new connection");
			this.connectionPool.addElement(createNewConnectionForPool(this.propertyFilePath));
		}
		log.info("MsSQLConnection pool is full");
	}
	
	/**
	 * This method checks if connection Pool is full or not
	 * @return boolean
	 */
	private synchronized boolean checkIfConnectionPoolIsFull() {
		final int MAX_POOL_SIZE = 1;
		if(this.connectionPool.size()<MAX_POOL_SIZE)
			return false;
		else
			return true;
	}
	
	public synchronized Connection getConnectionfromPool(){
			
			Connection con = null;
			if(this.connectionPool.size()>0){
				con = this.connectionPool.firstElement();
				this.connectionPool.removeElementAt(0);
			}
			return con;
	}
	
	public synchronized void returnConnectionToPool(Connection connection)
    {
        //Adding the connection from the client back to the connection pool
        this.connectionPool.addElement(connection);
    }
	
	public void destroyConnectionPool() {
		Enumeration<Connection> e = this.connectionPool.elements();
		while(e.hasMoreElements()) {
			Connection con = e.nextElement();
			try{
			con.close();
			}
			catch(SQLException sqle){
				log.error("SQLException", sqle);
			}
		}
		this.connectionPool.removeAllElements();
		log.info("All Mssql connections were destroyed");
	} 
	
	public Connection createNewConnectionForPool(String propertyFilePath)
	{
		
		Connection con = null;
		try {
			this.props.load(new FileInputStream(propertyFilePath));
		} catch (FileNotFoundException fe) {
			log.info("File not found error: " + fe);
		} catch (IOException e) {
			log.info("IOException error: " + e);
		}
		
		// PubAdmin Database
		this.url = this.props.getProperty("Fetch.database.url");
		this.username = this.props.getProperty("Fetch.database.username");
		this.password = this.props.getProperty("Fetch.database.password");

		
		
		if (!this.url.isEmpty() && !this.username.isEmpty() && !this.password.isEmpty()) {

					try {
						Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
						con = DriverManager.getConnection(this.url, this.username, this.password);
					} 
					catch(SQLException e) {
						// TODO Auto-generated catch block
						log.error("SQLException: " , e);
					} catch (ClassNotFoundException cnfe) {
						// TODO Auto-generated catch block
						log.error("ClassNotFoundException: " , cnfe);
					}
		}
		return con;
	}
	
	/**
	 * This function gets connection to asms instance
	 * @param propertyFilePath
	 * @return
	 */
	public static Connection getConnectionForAsms(String propertyFilePath)
	{
		Properties propsAsms = new Properties();
		Connection con = null;
		try {
			propsAsms.load(new FileInputStream(propertyFilePath));
		} catch (FileNotFoundException fe) {
			log.info("File not found error: " + fe);
		} catch (IOException e) {
			log.info("IOException error: " + e);
		}
		
		// PubAdmin Database
		String asmsurl = propsAsms.getProperty("ASMS.database.url");
		String asmsusername = propsAsms.getProperty("ASMS.database.username");
		String asmspassword = propsAsms.getProperty("ASMS.database.password");

		
		
		if (!asmsurl.isEmpty() && !asmsusername.isEmpty() && !asmspassword.isEmpty()) {

					try {
						Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
						con = DriverManager.getConnection(asmsurl, asmsusername, asmspassword);
					} 
					catch(SQLException e) {
						// TODO Auto-generated catch block
						log.error("SQLException: " , e);
					} catch (ClassNotFoundException cnfe) {
						// TODO Auto-generated catch block
						log.error("ClassNotFoundException: " , cnfe);
					}
		}
		return con;
	}
	
}
