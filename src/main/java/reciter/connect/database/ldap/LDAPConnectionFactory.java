/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/
package reciter.connect.database.ldap;

import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;

import javax.inject.Inject;

import com.unboundid.asn1.ASN1OctetString;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPSearchException;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;
import com.unboundid.ldap.sdk.controls.SimplePagedResultsControl;
import com.unboundid.util.ssl.SSLUtil;
import com.unboundid.util.ssl.TrustAllTrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class LDAPConnectionFactory {

	private static final Logger slf4jLogger = LoggerFactory.getLogger(LDAPConnectionFactory.class);

    private String ldapBindDn;
    private Integer ldapPort;
    private String ldapBindPassword;

    private String ldapHostname;

	private String ldapbaseDn;
    
    private Vector<LDAPConnection> connectionPool = new Vector<LDAPConnection>(); 
    
    /**
	 * @param propertyFilePath the path of property file
	 */
    @Inject
    @Autowired(required=true)
	public LDAPConnectionFactory(@Value("${ldap.bind.dn}") String ldapBindDn, @Value("${ldap.port}") int ldapPort, Environment env,
			@Value("${ldap.hostname}") String ldapHostname, @Value("${ldap.base.dn}") String ldapbaseDn) {
        this.ldapbaseDn = ldapbaseDn;
        this.ldapBindDn = ldapBindDn;
        this.ldapBindPassword = env.getProperty("LDAP_BIND_PASSWORD");
        this.ldapHostname = ldapHostname;
        this.ldapPort = ldapPort;
        initialize();
	}

    public LDAPConnection createConnection() {

		LDAPConnection connection = null;
		try {
			SSLUtil sslUtil = new SSLUtil(new TrustAllTrustManager());
			connection = new LDAPConnection(sslUtil.createSSLSocketFactory());
			connection.connect(ldapHostname, ldapPort);
			connection.bind(ldapBindDn, ldapBindPassword);

		} catch (LDAPException e) {
			slf4jLogger.error("LDAPConnection error", e);
		} catch (GeneralSecurityException e) {
			slf4jLogger.error("Error connecting via SSL to LDAP", e);
		}
		return connection;
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
			slf4jLogger.info("LDAP Connection pool is not full. Proceeding with adding new connection");
			this.connectionPool.addElement(getConnection());
		}
		slf4jLogger.info("LDAP Connection pool is full");
	}
	
	/**
	 * This method checks if connection Pool is full or not
	 * @return boolean
	 */
	private synchronized boolean checkIfConnectionPoolIsFull() {
		final int MAX_POOL_SIZE = 10;
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
		slf4jLogger.info("All LDAP connection was destroyed");
	} 
	
	public LDAPConnection getConnection() {
		
		LDAPConnection connection = null;
		try {
			SSLUtil sslUtil = new SSLUtil(new TrustAllTrustManager());
			connection = new LDAPConnection(sslUtil.createSSLSocketFactory());
			connection.connect(ldapHostname, ldapPort);
			connection.bind(ldapBindDn, ldapBindPassword);
			
		} catch (LDAPException e) {
			slf4jLogger.error("LDAPConnection error", e);
		} catch (GeneralSecurityException e) {
			slf4jLogger.error("Error connecting via SSL to LDAP", e);
		}
		return connection;
	}
	
	/**
	 * Searches the ED for the provided filter.
	 *
	 * @param filter A valid LDAP filter string
	 * @return a {@code List} of {@code SearchResultEntry} objects.
	 */
	public List<SearchResultEntry> search(final String filter) {
		return search(filter, ldapbaseDn, SearchScope.SUBORDINATE_SUBTREE, "*","modifyTimestamp");
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
		return search(filter, basedn, SearchScope.SUBORDINATE_SUBTREE, "*","modifyTimestamp");
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
     public List<SearchResultEntry> search(final String filter, final String base, SearchScope scope, String... attributes) {
         LDAPConnection connection = null;
         List<SearchResultEntry> entries = new ArrayList<>();
         
         // Log the search parameters for better visibility
         slf4jLogger.info("Starting LDAP search with filter: {}", filter);
         slf4jLogger.info("Search base DN: {}", base);
         slf4jLogger.info("Search scope: {}", scope);
         slf4jLogger.info("Requested attributes: {}", Arrays.toString(attributes));
         
         try {
             SearchRequest searchRequest = new SearchRequest(base, scope, filter, attributes);
             
             // Log additional SearchRequest properties
             slf4jLogger.info("Search time limit: {} seconds", searchRequest.getTimeLimitSeconds());
             slf4jLogger.info("Search size limit: {}", searchRequest.getSizeLimit());
             slf4jLogger.info("Search deref policy: {}", searchRequest.getDerefPolicy());
             
             ASN1OctetString resumeCookie = null;
     
             connection = getConnectionfromPool();
             if (connection != null) {
                 while (true) {
                     searchRequest.setControls(new SimplePagedResultsControl(500, resumeCookie));
                     SearchResult results = connection.search(searchRequest);
                     
                     // Log the SearchResult information
                     slf4jLogger.info("SearchResult: {}", results);
                     
                     entries.addAll(results.getSearchEntries());
                     SimplePagedResultsControl responseControl = SimplePagedResultsControl.get(results);
                     if (responseControl.moreResultsToReturn()) {
                         resumeCookie = responseControl.getCookie();
                     } else {
                         break;
                     }
                 }
             }
         } catch (LDAPSearchException e) {
             slf4jLogger.error("LDAPSearchException", e);
         } catch (LDAPException e) {
             slf4jLogger.error("LDAPException", e);
         } finally {
             if (connection != null) {
                 returnConnectionToPool(connection);
             }
         }
         return entries;
     }
}
