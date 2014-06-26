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

import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.spi.ConfigurationProperty;

public class SASConfiguration extends AbstractConfiguration {

    private static final Log LOG = Log.getLog(SASConfiguration.class);

    private String hostname; 
    private String port; 
    private String username; 
    private GuardedString password; 

    @Override
    public void validate() {
        if (StringUtil.isEmpty(hostname)){
        	throw new ConfigurationException("Server name is not specified.");
        }
        
        if (StringUtil.isEmpty(port)){
        	throw new ConfigurationException("Server port is not specified.");
        }
        
        if (StringUtil.isEmpty(username)){
        	throw new ConfigurationException("User is not defined.");
        }
        
        
        if (password == null){
        	throw new ConfigurationException("Pasword value is not specified.");
        }
    }

    
    public String getHostname() {
		return hostname;
	}
    
    public void setHostname(String host) {
		this.hostname = host;
	}
    
    public String getPort() {
		return port;
	}
    public void setPort(String port) {
		this.port = port;
	}
    
   public GuardedString getPassword() {
	   return password;
   }
   
   public void setPassword(GuardedString password) {
	   this.password = password;
   }
    
    public String getUsername() {
		return username;
	}
    
    public void setUsername(String username) {
		this.username = username;
	}
//    @ConfigurationProperty(displayMessageKey = "${connectorNameLowerCase}.config.sampleProperty",
//            helpMessageKey = "${connectorNameLowerCase}.config.sampleProperty.help")
//    public String getSampleProperty() {
//        return sampleProperty;
//    }
//
//    public void setSampleProperty(String sampleProperty) {
//        this.sampleProperty = sampleProperty;
//    }
}