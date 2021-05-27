
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
package reciter.connect.database.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Vector;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * @author szd2013
 * This class manages connection to all oracle datasources
 *
 */
@Slf4j
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class MysqlConnectionFactory {

	private String username;
	private String password;
	private String url;
	
	private Vector<Connection> connectionPool = new Vector<Connection>(); 
	
	/**
	 * @param propertyFilePath the path of property file
	 */
	@Inject
	@Autowired(required=true)
	public MysqlConnectionFactory(@Value("${mysql.vivo.coi.db.username}") String username, Environment env, @Value("${mysql.vivo.coi.db.url}") String url) {
		this.username = username;
		this.password = env.getProperty("VIVO_COI_DB_PASSWORD");
		this.url = url;
		//initialize();
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
			log.info("MySQLConnection pool is not full. Proceeding with adding new connection");
			this.connectionPool.addElement(createNewConnectionForPool());
		}
		log.info("MySQLConnection pool is full");
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
		log.info("All Mysql connections were destroyed");
	} 
	
	public Connection createNewConnectionForPool()
	{
		
		Connection con = null;
		
		if (!this.url.isEmpty() && !this.username.isEmpty() && !this.password.isEmpty()) {

					try {
						con = DriverManager.getConnection(this.url, this.username, this.password);
					} 
					catch(SQLException e) {
						log.error("SQLException: " , e);
					} 
		}
		return con;
	}
}