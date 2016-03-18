/*-
 * #%L
 * com.paremus.cluster.api
 * %%
 * Copyright (C) 2016 - 2019 Paremus Ltd
 * %%
 * Licensed under the Fair Source License, Version 0.9 (the "License");
 * 
 * See the NOTICE.txt file distributed with this work for additional 
 * information regarding copyright ownership. You may not use this file 
 * except in compliance with the License. For usage restrictions see the 
 * LICENSE.txt file distributed with this work
 * #L%
 */
package com.paremus.cluster;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import org.osgi.annotation.versioning.ProviderType;

/**
 * This service is provided by the cluster implementation and it
 * provides information about the cluster. It is always registered
 * with the property <code>cluster.name</code> set to the name of
 * the cluster.
 */
@ProviderType
public interface ClusterInformation {

	/**
	 * Get the known members of the cluster. Each UUID is the framework id
	 * of the remote OSGi framework
	 * @return A collection containing the known cluster members
	 */
	Collection<UUID> getKnownMembers();

	/**
	 * Get the IP addresses for each of the members of the cluster
	 * 
	 * @return A map of member ids to the address that they appear to be from 
	 */
	Map<UUID, InetAddress> getMemberHosts();
	
	/**
	 * Get the name of this cluster
	 * @return the cluster name
	 */
	String getClusterName();
	
	/**
	 * Get the IP address for a specific member
	 * @param member the member to query
	 * @return the IP address
	 */
	InetAddress getAddressFor(UUID member);
	
	/**
	 * Get the UUID of this cluster member
	 * @return the local UUID
	 */
	UUID getLocalUUID();
	
	/**
	 * Get the stored byte data for a given member
	 * @param member the member to query
	 * @return the key to byte values stored by this member
	 */
	Map<String, byte[]> getMemberAttributes(UUID member);

	/**
	 * Get the stored byte data for a given key
	 * from a given member
	 * @param member the member to query
	 * @param key the property key
	 * @return the bytes stored by the member for this key
	 */
	byte[] getMemberAttribute(UUID member, String key);
	
	/**
	 * Advertise the named attribute with the supplied data
	 * @param key the property name
	 * @param data the data to advertise, or <code>null</code>
	 *  to remove previously advertised data for the key.
	 */
	void updateAttribute(String key, byte[] data);
	
}
