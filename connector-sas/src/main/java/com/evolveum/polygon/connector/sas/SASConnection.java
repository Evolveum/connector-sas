/*
 * Copyright (c) 2010-2014 Evolveum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.evolveum.polygon.connector.sas;

import java.rmi.ConnectException;
import java.rmi.RemoteException;

import javax.naming.ConfigurationException;

import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.operations.ValidateApiOp;
import org.identityconnectors.framework.common.exceptions.ConnectionBrokenException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;

import com.sas.iom.SASIOMCommon.ServerState;
import com.sas.meta.SASOMI.IOMI;
import com.sas.metadata.remote.MdException;
import com.sas.metadata.remote.MdFactory;
import com.sas.metadata.remote.MdFactoryImpl;
import com.sas.metadata.remote.MdOMRConnection;

public class SASConnection {

    private static final Log LOG = Log.getLog(SASConnection.class);

    private SASConfiguration configuration;
    
    private MdFactory factory;
    private MdOMRConnection connection;

    public SASConnection(SASConfiguration configuration) {
        this.configuration = configuration;
//        getFactory();
    }
    
    public void validate() throws RemoteException, MdException{
    	MdOMRConnection conn = getConnection();
    	
    	if (MdOMRConnection.SERVER_STATUS_OK != conn.getServerStatus()){
    		throw new ConnectionBrokenException("Could not connect to the server");
    	}
    	
    	
    	
    }
    
    private String clearValuePassword;
    
    public void setClearValuePassword(String clearValuePassword) {
		this.clearValuePassword = clearValuePassword;
	}
    
    public String getClearValuePassword() {
		return clearValuePassword;
	}
    
    MdFactory getFactory() {
		if (factory == null){
			try {
				factory = new MdFactoryImpl();
				
				configuration.getPassword().access(new GuardedString.Accessor() {
						public void access(char[] arg0) {
							setClearValuePassword(new String(arg0));
					}
				});
				if (StringUtil.isBlank(clearValuePassword)){
					throw new ConnectorException("Could not connect to the SAS Metadata server. No password for user provided.");
				}
				factory.makeOMRConnection(configuration.getHostname(), configuration.getPort(), configuration.getUsername(), clearValuePassword);
				//TODO: maybe initialize factory??
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				throw new ConnectorException(e.getMessage());
			} catch (MdException e) {
				throw new ConnectorException(e.getMessage(), e);
			}
		}
		return factory;
	}

    public MdOMRConnection getConnection() throws java.rmi.ConnectException {
    	if (connection == null){
    		
    		try {
    			connection = getFactory().getConnection();
				
			
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				throw new java.rmi.ConnectException(e.getMessage(), e);
			}
    	}
    	return connection;
	}
    
    public void dispose() {
        //todo implement
    }
}