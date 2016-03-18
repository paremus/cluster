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
package com.paremus.net.info;

import java.net.InetAddress;
import java.util.UUID;

import org.osgi.annotation.versioning.ProviderType;

@ProviderType
public interface ClusterNetworkInformation {
	
	/**
	 * The bind address that this cluster is using
	 * 
	 * @return the address to which this server is bound
	 */
	public InetAddress getBindAddress();

	/**
	 * Our best guess as to whether there is a NAT firewall between us 
	 * and the remote node
	 * @return true if a NAT firewall has been detected
	 */
	public boolean isFirewalled();
	
	/**
	 * Get the address of this fibre as seen by the
	 * outside world
	 * 
	 * @return The InetAddress of this fibre as used
	 *  by a remote node to communicate with us
	 */
	public InetAddress getFibreAddress();
	
	/**
	 * Get the name of this cluster
	 * @return the cluster name
	 */
	public String getClusterName();

	/**
	 * Get the UUID of this cluster member
	 * @return the local UUID
	 */
	public UUID getLocalUUID();
	
}
