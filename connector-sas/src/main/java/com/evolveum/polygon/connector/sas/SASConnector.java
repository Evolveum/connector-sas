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

import java.io.ObjectInputStream.GetField;
import java.net.ConnectException;
import java.rmi.ConnectIOException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.api.operations.CreateApiOp;
import org.identityconnectors.framework.common.exceptions.AlreadyExistsException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeInfo.Flags;
import org.identityconnectors.framework.common.objects.AttributeInfoBuilder;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfo;
import org.identityconnectors.framework.common.objects.ObjectClassInfoBuilder;
import org.identityconnectors.framework.common.objects.ObjectClassUtil;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.AbstractFilterTranslator;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.CreateOp;
import org.identityconnectors.framework.spi.operations.DeleteOp;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.SearchOp;
import org.identityconnectors.framework.spi.operations.TestOp;
import org.identityconnectors.framework.spi.operations.UpdateAttributeValuesOp;
import org.identityconnectors.framework.spi.operations.UpdateOp;

import com.sas.datatypes.DateType;
import com.sas.metadata.remote.CMetadata;
import com.sas.metadata.remote.MdException;
import com.sas.metadata.remote.MdFactory;
import com.sas.metadata.remote.MdFactoryImpl;
import com.sas.metadata.remote.MdOMIUtil;
import com.sas.metadata.remote.MdOMRConnection;
import com.sas.metadata.remote.MdObjectAttribute;
import com.sas.metadata.remote.MdObjectStore;
import com.sas.metadata.remote.MetadataObjects;
import com.sas.metadata.remote.MetadataState;
import com.sas.metadata.remote.Person;
import com.sas.metadata.remote.impl.CMetadataImpl;
import com.sas.metadata.remote.impl.PersonImpl;
import com.sas.metadata.remote.impl.RootImpl;
import com.sas.util.Classes;
import com.sas.util.DateTypes;

@ConnectorClass(displayNameKey = "sas.connector.display", configurationClass = SASConfiguration.class)
public class SASConnector implements Connector, TestOp, SchemaOp, CreateOp, UpdateAttributeValuesOp, DeleteOp, SearchOp<String> {

    private static final Log LOG = Log.getLog(SASConnector.class);

    private SASConfiguration configuration;
    private SASConnection connection;
    
    private MdOMIUtil omiUtil;
    
    public Configuration getConfiguration() {
        return configuration;
    }

    public void init(Configuration configuration) {
        this.configuration = (SASConfiguration)configuration;
        this.connection = new SASConnection(this.configuration);
    }
    
    

    public void dispose() {
        configuration = null;
        if (connection != null) {
            connection.dispose();
            connection = null;
        }
    }
    
    private MdOMIUtil getOmiUtil() throws RemoteException {
    	if (omiUtil == null){
    		omiUtil = connection.getFactory().getOMIUtil();
    	}
		return omiUtil;
	}
    
    private MdObjectStore createObjectStore() throws RemoteException{
    	return connection.getFactory().createObjectStore();
    }
    
    private CMetadata createComplexMetadataObject(MdObjectStore objectStore, String objectName, String metadataType, String repoId) throws RemoteException{
    	return connection.getFactory().createComplexMetadataObject(objectStore, objectName, MetadataObjects.PERSON, repoId);
    }
    
    private String getFoundationRepoId() throws RemoteException, MdException{
    	return getOmiUtil().getFoundationRepository().getFQID();
    }
    
    private String getXmlSelectForName(String name){
    	StringBuilder xmlSelectBuilder = new StringBuilder("<XMLSELECT search=");
		xmlSelectBuilder.append("\"Person[@Name='");
		xmlSelectBuilder.append(name);
		xmlSelectBuilder.append("']\"/>");
		return xmlSelectBuilder.toString();
    }
 
	public Uid create(ObjectClass objectClass, Set<Attribute> attributes,
			OperationOptions arg2) {
		// TODO Auto-generated method stub
		LOG.ok("Start creating object");
		if (isAccount(objectClass)){
			Name name = AttributeUtil.getNameFromAttributes(attributes);
			if (name == null){
				throw new IllegalArgumentException("Error creating object, no name specified.");
			}
			
			LOG.ok("Determined name from attributes {0}", name.getNameValue());
			
		 
		try {
			
			
			
			MdObjectStore objectStore = createObjectStore();
			
			String repoId = getFoundationRepoId();
			
			String shortReposID = repoId.substring(repoId.indexOf('.') + 1, 
                    repoId.length());
			
			String xmlSelect = getXmlSelectForName(name.getNameValue());

			int exist = getOmiUtil().doesObjectExist(repoId, MetadataObjects.PERSON, xmlSelect);
			if (exist != 0){
				throw new AlreadyExistsException("Object with the same name "+name.getNameValue()+" already exists.");
			}
			
			LOG.ok("Determined (foundation) repository id {0}", shortReposID);
			Person myObject = (Person) createComplexMetadataObject(objectStore, name.getNameValue(), MetadataObjects.PERSON, shortReposID);
			myObject.setAttrs(getAttributeChanges(attributes));
			
			 myObject.updateMetadataAll();  // Write object to server
			 String stringUid = myObject.getId();
			 LOG.ok("Object uid - after object was created {0}", stringUid);
			 Uid uid = new Uid(stringUid);
			 objectStore.dispose();  // dispose of the object store if it is no longer needed
			 return uid;
		} catch (java.rmi.ConnectException e) {
			// TODO Auto-generated catch block
			throw new ConnectorException(e);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			throw new ConnectorException(e);
		} catch (MdException e) {
			throw new ConnectorException(e);
		}
		
		
		} else {
			throw new ConnectorException("Could not create object. Unsupported object class " + objectClass);
		}
	}

	private boolean isAccount(ObjectClass objectClass){
		if (objectClass == null){
			return false;
		}
		
		if (ObjectClass.ACCOUNT.is(objectClass.getObjectClassValue())){
			return true;
		}
		
		return false;
	}

	public Uid update(ObjectClass objClass, Uid uid, Set<Attribute> attributes,
			OperationOptions opts) {
		if (isRename(attributes)){
			delete(objClass, uid, opts);
			return create(objClass, attributes, opts);
		}
		Map<String, MdObjectAttribute> changedAttributes = getAttributeChanges(attributes);
		return doUpdate(objClass, uid, changedAttributes, opts);
	}
	
	private Uid doUpdate(ObjectClass objClass, Uid uid, Map<String, MdObjectAttribute> changedAttributes,
			OperationOptions arg3){
		if (!isAccount(objClass)){
			throw new ConnectorException("Could not delete record from object class : " + objClass + ". Expected account object class.");
		}
		
		LOG.ok("Update object start");
	
		
		String objId = uid.getUidValue();
		LOG.ok("Updating object with uid {0}", uid.getUidValue());
		
		try {
			MdObjectStore objectStore = createObjectStore();
			CMetadata metadata = getOmiUtil().getFullObject(objectStore, MetadataObjects.PERSON, objId);
			if (metadata == null){
				throw new NoSuchObjectException("Object with the identifier " +objId+ " does not exist.");
			}
			
			String xmlSelect = getXmlSelectForName(metadata.getName());
			int exist = getOmiUtil().doesObjectExist(getFoundationRepoId(), MetadataObjects.PERSON, xmlSelect);
			if (exist == 0){
				throw new NoSuchObjectException("Object with name " +metadata.getName()+ " does not exist.");
			}
			
			metadata = createComplexMetadataObject(objectStore, metadata.getName(), MetadataObjects.PERSON, objId);
			LOG.ok("Found metadata object {0}", metadata.toString());
			
			metadata.setAttrs(changedAttributes);
			metadata.updateMetadataAll();
			String uidAfterChange = metadata.getId();
			objectStore.dispose();
			return new Uid(uidAfterChange);
		} catch (java.rmi.ConnectException e) {
			throw new ConnectorException(e);
		} catch (RemoteException e) {
			throw new ConnectorException(e);
		} catch (MdException e){
			throw new ConnectorException(e);
		}
		
                
		
	}

	public void test() {
		configuration.validate();
		try {
			connection.validate();
		} catch (RemoteException e) {
			throw new ConnectorException(e);
		} catch (MdException e) {
			throw new ConnectorException(e);
		}
	}

	public Schema schema() {

		SchemaBuilder schemaBuilder = new SchemaBuilder(SASConnector.class);
		ObjectClassInfo accountObjectClassInfo = createObjectClassInfo(ObjectClass.ACCOUNT_NAME, MetadataObjects.PERSON);
		ObjectClassInfo groupObjectClassInfo = createObjectClassInfo(ObjectClass.GROUP_NAME, MetadataObjects.GROUP);
		schemaBuilder.defineObjectClass(accountObjectClassInfo);
		schemaBuilder.defineObjectClass(groupObjectClassInfo);
		
//		List<String> metadataObjects = new ArrayList<String>();
//		try {
//			connection.getFactory().getOMIUtil().getTypes(metadataObjects, new ArrayList<String>());
//			
//			for (String metadataObject : metadataObjects){
//				LOG.ok("Gnerating object class for metadata object {0}" , metadataObject);
//				if (MetadataObjects.PERSON.equals(metadataObject)){
//					continue;
//				}
//				if (MetadataObjects.GROUP.equals(metadataObject)){
//					continue;
//				}
//				//let the icf object class the same as metadata object specified..it will generate prefix complex and sufix obejctclass
//				ObjectClassInfo objClassInfo = createObjectClassInfo(metadataObject, metadataObject);
//				schemaBuilder.defineObjectClass(objClassInfo);
//			}
//				
//		} catch (RemoteException e) {
//			throw new ConnectorException(e);
//		} catch (MdException e) {
//			throw new ConnectorException(e);
//		}
		
		
		return schemaBuilder.build();
	}
	
	private static Map<String, Class> classMapping = new HashMap<String, Class>();
	static{
		classMapping.put("String", String.class);
		//TODO: use other type..for testing purposes this is OK
		classMapping.put("DateTime", String.class);
		classMapping.put("Int", Integer.class);
		classMapping.put("Double", Double.class);
	}
	
	private ObjectClassInfo createObjectClassInfo(String icfObjectClass, String sasMetadataObjectClass){
		ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
		objClassBuilder.setType(icfObjectClass);
		
		Map<String, Map<String, String>> attributeMap = new HashMap<String, Map<String, String>>();
		Map<String, Map<String, String>> associationMap = new HashMap<String, Map<String, String>>();
		try {
			getOmiUtil().getTypeProperties(sasMetadataObjectClass, attributeMap, associationMap);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			throw new ConnectorException(e);
		} catch (MdException e) {
			// TODO Auto-generated catch block
			throw new ConnectorException(e);
		}
		
		List<AttributeInfo> attrKeys = new ArrayList<AttributeInfo>();
		
		for (String key : attributeMap.keySet()){
			if (CMetadata.ATTRIBUTE_NAME_NAME.equals(key)){
				continue;
			}
			
			if (CMetadata.ATTRIBUTE_ID_NAME.equals(key)){
				continue;
			}
		
			Map<String, String> vals = attributeMap.get(key);
			
			AttributeInfoBuilder attrBuilder = new AttributeInfoBuilder(key);
			String type = vals.get("Type");
			Class clazz = classMapping.get(type);
			if (clazz == null){
				LOG.ok("No mapping found for {0}", type);
				clazz = String.class;
				
			}
			attrBuilder.setType(clazz);
			AttributeInfo attr = attrBuilder.build();
			attrKeys.add(attr);
		}
				
		objClassBuilder.addAllAttributeInfo(attrKeys);
		return objClassBuilder.build();
	}

	public FilterTranslator<String> createFilterTranslator(ObjectClass arg0,
			OperationOptions arg1) {
		
		return new AbstractFilterTranslator() {
		};
	}

	public void executeQuery(ObjectClass objClass, String query,
			ResultsHandler handler, OperationOptions options) {
		// TODO Auto-generated method stub
		LOG.ok("Query {0} ", query);
		
		if (!isAccount(objClass)){
			throw new UnsupportedOperationException("Could not execute query. Object class not supported: " + objClass);
		}
		
		try {
			String repoId = getFoundationRepoId();
			
			String template = "<Templates>" + 
                    "<Person>" +
                    "</Person>" +
                  "</Templates>";
			
			int flags = MdOMIUtil.OMI_GET_METADATA | MdOMIUtil.OMI_ALL |
                    MdOMIUtil.OMI_TEMPLATE;
			List objects = connection.getFactory().getOMIUtil().getMetadataObjects(repoId, MetadataObjects.PERSON, flags, template);
	
			Iterator iterator = objects.iterator();
			while (iterator.hasNext()){
				Object obj = iterator.next();
				RootImpl p = (RootImpl) obj; 
				ConnectorObjectBuilder cob = new ConnectorObjectBuilder();
				 
				cob.addAttribute(Name.NAME, p.getName());
				cob.addAttribute(Uid.NAME, p.getId());
				
				Set<Entry<String, String>> attrs = p.getAttrs().entrySet();
				Iterator<Entry<String, String>> attrIterator = attrs.iterator();
				while (attrIterator.hasNext()){
					Entry<String, String> attr = attrIterator.next();
					//skip id and name..it was set before (icf_name and icf_uid)
					if (attr.getKey().equals(CMetadata.ATTRIBUTE_ID_NAME)){
						continue;
					}
					
					if (attr.getKey().equals(CMetadata.ATTRIBUTE_NAME_NAME)){
						continue;
					}
					if (StringUtil.isEmpty(attr.getValue())){
						LOG.ok("Skipping setting attribute {0}. Attribute value is blank", attr.getKey());
					} else {
						LOG.ok("Setting attribute {0} with value {1}", attr.getKey(), attr.getValue());
						cob.addAttribute(attr.getKey(), attr.getValue());
					}
				}
				
				handler.handle(cob.build());
			}
				
			
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			throw new ConnectorException(e);
		} catch (MdException e) {
			// TODO Auto-generated catch block
			throw new ConnectorException(e);
		}
				
		
	}

	public void delete(ObjectClass objClass, Uid uid, OperationOptions arg2) {
		
		try {
			
			if (!isAccount(objClass)){
				throw new ConnectorException("Could not delete record from object class : " + objClass + ". Expected account object class.");
			}
			
			LOG.ok("Attempt to delete object with uid {0}", uid.getUidValue());
			
			getOmiUtil().deleteMetadataObject(MetadataObjects.PERSON, uid.getUidValue());
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			throw new ConnectorException(e);
		} catch (MdException e) {
			// TODO Auto-generated catch block
			throw new ConnectorException(e);
		}
		
	}

	public Uid addAttributeValues(ObjectClass objClass, Uid uid,
			Set<Attribute> attributes, OperationOptions opts) {
		 Map<String, MdObjectAttribute> changedAttributes = getAttributeChanges(attributes);
		return doUpdate(objClass, uid, changedAttributes, opts);
	}

	public Uid removeAttributeValues(ObjectClass objClass, Uid uid,
			Set<Attribute> attributes, OperationOptions opts) {
		if (isRename(attributes)){
			throw new ConnectorException("Name attribute could not be removed.");
		}
		Map<String, MdObjectAttribute> changedAttributes = getAttributeChanges(attributes);
		return doUpdate(objClass, uid, changedAttributes, opts);
	}
	
	private Map<String, MdObjectAttribute> getAttributeChanges(Set<Attribute> attributes){
		Map<String, MdObjectAttribute> changes = new HashMap<String, MdObjectAttribute>();
		for (Attribute attr : attributes){
			if (!attr.getValue().isEmpty()){
				MdObjectAttribute a = new MdObjectAttribute(attr.getName(), MetadataState.LOCAL, attr.getValue().iterator().next().toString());
				changes.put(attr.getName(), a);
			} else {
				MdObjectAttribute a = new MdObjectAttribute(attr.getName(), MetadataState.LOCAL, null);
				changes.put(attr.getName(), a);
			}

		}
		return changes;
	}
	
	private boolean isRename(Set<Attribute> attributes){
		for (Attribute attr : attributes){
			if (Name.NAME.equals(attr.getName())){
				return true;
			}
		}
		return false;
	}

}
