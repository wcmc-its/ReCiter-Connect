package org.vivoweb.harvester.connectionfactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPSearchException;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;
import com.unboundid.util.ssl.SSLUtil;
import com.unboundid.util.ssl.TrustAllTrustManager;

/**
 * @author Sarbajit Dutta <szd2013@med.cornell.edu>
 * <p><b><i>This class creates and manages connections for LDAP related connections. You can create , get , return and remove connection from pool with different
 * functional methods.<p><b><i>
 *
 */
public class LDAPConnectionFactory {
	
	private static Properties props = new Properties();
	private static Logger logger = LoggerFactory.getLogger(LDAPConnectionFactory.class);

	private static String ldapBindDn = null;
	private static String ldapBindPassword = null;
	private static String ldapHostname = null;
	private static int ldapPort = 0;
	private static String baseDN = null; 
	
	private String propertyFilePath = null;
	
	private Vector<LDAPConnection> connectionPool = new Vector<LDAPConnection>(); 
	
	private static LDAPConnectionFactory instance = null;
	
	protected LDAPConnectionFactory() {
		
	}
	
	/**
	 * @param propertyFilePath the path of property file
	 * @return an instance of LDAP connection
	 * This method returns a singleton object
	 */
	public static LDAPConnectionFactory getInstance(String propertyFilePath) {
		if(instance == null)
			instance = new LDAPConnectionFactory(propertyFilePath);
		
		return instance;
	}
	
	
	/**
	 * @param propertyFilePath the path of property file
	 */
	public LDAPConnectionFactory(String propertyFilePath) {
		this.propertyFilePath = propertyFilePath;
		initialize();
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
			logger.info("LDAP Connection pool is not full. Proceeding with adding new connection");
			this.connectionPool.addElement(getConnection(this.propertyFilePath));
		}
		logger.info("LDAP Connection pool is full");
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
	
	
	/**
	 * This method gets a connection from pool and returns it
	 * @return LDAP Connection
	 */
	public synchronized LDAPConnection getConnectionfromPool(){
		
		LDAPConnection con = null;
		if(this.connectionPool.size()>0){
			con = this.connectionPool.firstElement();
			this.connectionPool.removeElementAt(0);
		}
		return con;
	}
	
	/**
	 * This method returns the connection to the pool after its used
	 * @param connection LDAP Connection
	 */
	public synchronized void returnConnectionToPool(LDAPConnection connection)
    {
        //Adding the connection from the client back to the connection pool
        this.connectionPool.addElement(connection);
    }
	
	public void destroyConnectionPool() {
		Enumeration<LDAPConnection> e = this.connectionPool.elements();
		while(e.hasMoreElements()) {
			LDAPConnection con = e.nextElement();
			if(con!= null)
				con.close();
		}
		this.connectionPool.removeAllElements();
		logger.info("All LDAP connection was destroyed");
	} 
	
	public static LDAPConnection getConnection(String propertyFilePath) {
		try {
			props.load(new FileInputStream(propertyFilePath));
		} catch (FileNotFoundException fe) {
			logger.info("File not found error: " + fe);
		} catch (IOException e) {
			logger.info("IOException error: " + e);
		}
		
		ldapBindDn = props.getProperty("bindDN");
		ldapBindPassword = props.getProperty("bindPassword");
		baseDN = props.getProperty("ldapBaseDN");
		ldapHostname = props.getProperty("ldapHostname");
		ldapPort = Integer.parseInt(props.getProperty("ldapPort"));
		
		LDAPConnection connection = null;
		try {
			SSLUtil sslUtil = new SSLUtil(new TrustAllTrustManager());
			connection = new LDAPConnection(sslUtil.createSSLSocketFactory());
			connection.connect(ldapHostname, ldapPort);
			connection.bind(ldapBindDn, ldapBindPassword);
			
		} catch (LDAPException e) {
			logger.error("LDAPConnection error", e);
		} catch (GeneralSecurityException e) {
			logger.error("Error connecting via SSL to LDAP", e);
		}
		return connection;
	}
	
	/**
	 * Searches the ED for the provided filter.
	 *
	 * @param filter A valid LDAP filter string
	 * @return a {@code List} of {@code SearchResultEntry} objects.
	 */
	public List<SearchResultEntry> search(final String filter, String propertyFilePath) {
		return search(filter, this.baseDN, propertyFilePath, SearchScope.SUBORDINATE_SUBTREE, "*","modifyTimestamp");
	}

	/**
	 * Searches the ED for the provided filter.
	 *
	 * @param filter A valid LDAP filter string
	 * @param basedn This is the basedn used to search the filter
	 * @param propertyFilePath This is the file path of the property file which contains the connection info
	 * @return a {@code List} of {@code SearchResultEntry} objects.
	 */
	public List<SearchResultEntry> searchWithBaseDN(final String filter, String basedn) {
		return search(filter, basedn, propertyFilePath, SearchScope.SUBORDINATE_SUBTREE, "*","modifyTimestamp");
	}

	/**
	 * Searches the ED for the provided filter.
	 *
	 * @param filter        A valid LDAP filter string
	 * @param base          The LDAP search base you want to use
	 * @param propertyFilePath This is the file path of the property file which contains the connection info
	 * @param scope         A SearchScope that you want to use
	 * @param attributes    A list of attributes that you want returned from
	 * LDAP
	 * @return a {@code List} of {@code SearchResultEntry} objects.
	 */
	public List<SearchResultEntry> search(final String filter, final String base, String propertyFilePath, SearchScope scope, String... attributes) {
		LDAPConnection connection = null;
		try {
			SearchRequest searchRequest = new SearchRequest(base, scope, filter, attributes);
			//connection = LDAPConnectionFactory.getConnection(propertyFilePath);
			connection = getConnectionfromPool();
			SearchResult results = connection.search(searchRequest);
			List<SearchResultEntry> entries = results.getSearchEntries();
			return entries;
		} catch (LDAPSearchException e) {
			logger.error("LDAPSearchException", e);
		} catch (LDAPException e) {
			logger.error("LDAPException", e);
		} finally {
			if (connection != null) {
				returnConnectionToPool(connection);
			}
		}
		return Collections.emptyList();
	}
	
	
}
