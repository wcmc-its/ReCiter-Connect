
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
package reciter.connect.database.mssql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * @author szd2013
 * This class manages connection to all mssql datasources
 *
 */
@Slf4j
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class MssqlConnectionFactory {

	private String username;
	private String password;
	private String url;
	
	private String infoEdUsername;
	private String infoEdPassword;
	private String infoEdUrl;
	
	private List<Connection> asmsConnectionPool = new ArrayList<>(); 
	private List<Connection> infoEdConnectionPool = new ArrayList<>(); 
	
	/**
	 * @param propertyFilePath the path of property file
	 */
	@Inject
	@Autowired(required=true)
	public MssqlConnectionFactory(@Value("${msssql.asms.db.username}") String username, @Value("${msssql.infoed.db.username}") String infoEdUsername, Environment env) {
		this.username = username;
		this.password = env.getProperty("MSSQL_ASMS_DB_PASSWORD");
		this.url = env.getProperty("MSSQL_ASMS_DB_URL");
		
		this.infoEdUsername = infoEdUsername;
		this.infoEdPassword = env.getProperty("MSSQL_INFOED_DB_PASSWORD");
		this.infoEdUrl = env.getProperty("MSSQL_INFOED_DB_URL");
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
		while(!checkIfAsmsConnectionPoolIsFull()) {
			log.info("MSSQLConnection pool is not full. Proceeding with adding new connection");
			asmsConnectionPool.add(createNewConnectionForPool("ASMS"));
		}

		while(!checkIfInfoEdConnectionPoolIsFull()) {
			log.info("MSSQLConnection pool is not full. Proceeding with adding new connection");
			infoEdConnectionPool.add(createNewConnectionForPool("INFOED"));
		}
		log.info("MSSQLConnection pool is full");
	}
	
	/**
	 * This method checks if connection Pool is full or not
	 * @return boolean
	 */
	private synchronized boolean checkIfAsmsConnectionPoolIsFull() {
		final int MAX_POOL_SIZE = 20;
		if(this.asmsConnectionPool.size()<MAX_POOL_SIZE)
			return false;
		else
			return true;
	}

	/**
	 * This method checks if connection Pool is full or not
	 * @return boolean
	 */
	private synchronized boolean checkIfInfoEdConnectionPoolIsFull() {
		final int MAX_POOL_SIZE = 15;
		if(this.infoEdConnectionPool.size()<MAX_POOL_SIZE)
			return false;
		else
			return true;
	}
	
	public synchronized Connection getInfoEdConnectionfromPool(String application){
			
			Connection con = null;
			if(this.infoEdConnectionPool.size()>0){
				con = this.infoEdConnectionPool.get(0);
				this.infoEdConnectionPool.remove(0);
			}
			return con;
	}

	public synchronized Connection getAsmsConnectionfromPool(String application){
			
		Connection con = null;
		if(this.asmsConnectionPool.size()>0){
			con = this.asmsConnectionPool.get(0);
			this.asmsConnectionPool.remove(0);
		}
		return con;
}
	
	public synchronized void returnConnectionToPool(String application, Connection connection)
    {
		//Adding the connection from the client back to the connection pool
		if(application.equals("ASMS"))
			this.asmsConnectionPool.add(connection);
		
		if(application.equals("INFOED"))
			this.infoEdConnectionPool.add(connection);

    }
	
	public void destroyConnectionPool() {
		Collection<Connection> e = this.asmsConnectionPool;
		for(Connection con: e) {
			try{
			con.close();
			}
			catch(SQLException sqle){
				log.error("SQLException", sqle);
			}
		}
		this.asmsConnectionPool.clear();

		e = this.infoEdConnectionPool;
		for(Connection con: e) {
			try{
			con.close();
			}
			catch(SQLException sqle){
				log.error("SQLException", sqle);
			}
		}
		this.infoEdConnectionPool.clear();
		log.info("All MSsql connections were destroyed");
	} 
	
	public Connection createNewConnectionForPool(String application)
	{
		
		Connection con = null;
		
		if (!this.url.isEmpty() && !this.username.isEmpty() && !this.password.isEmpty() && application.equalsIgnoreCase("ASMS")) {

					try {
						Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
						con = DriverManager.getConnection(this.url, this.username, this.password);
					} 
					catch(SQLException e) {
						log.error("SQLException: " , e);
					} catch (ClassNotFoundException cnfe) {
						log.error("ClassNotFoundException: " , cnfe);
					}
		}
		
		if (!this.infoEdUrl.isEmpty() && !this.infoEdUsername.isEmpty() && !this.infoEdPassword.isEmpty() && application.equalsIgnoreCase("INFOED")) {

			try {
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
				con = DriverManager.getConnection(this.infoEdUrl, this.infoEdUsername, this.infoEdPassword);
			} 
			catch(SQLException e) {
				log.error("SQLException: " , e);
			} catch (ClassNotFoundException cnfe) {
				log.error("ClassNotFoundException: " , cnfe);
			}
}
		return con;
	}

}
