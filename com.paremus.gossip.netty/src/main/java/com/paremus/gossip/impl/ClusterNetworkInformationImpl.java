/*-
 * #%L
 * com.paremus.gossip.netty
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
package com.paremus.gossip.impl;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.UUID;

import com.paremus.gossip.GossipComms;
import com.paremus.net.info.ClusterNetworkInformation;

public class ClusterNetworkInformationImpl implements ClusterNetworkInformation {

	private final InetAddress fibreAddress;

	private final String clusterName;
	
	private final GossipComms comms;

	private final boolean firewalled;
	
	private final UUID id;
	
	public ClusterNetworkInformationImpl(InetAddress fibreAddress, String clusterName,
			GossipComms comms, UUID id) {
		this.fibreAddress = fibreAddress;
		this.clusterName = clusterName;
		this.comms = comms;
		boolean firewalled;
		try {
			firewalled = NetworkInterface.getByInetAddress(fibreAddress) == null;
		} catch (SocketException se) {
			firewalled = true;
		}
		this.firewalled = firewalled;
		this.id = id;
	}

	@Override
	public InetAddress getBindAddress() {
		return comms.getBindAddress();
	}

	@Override
	public boolean isFirewalled() {
		return firewalled;
	}

	@Override
	public InetAddress getFibreAddress() {
		return fibreAddress;
	}

	@Override
	public String getClusterName() {
		return clusterName;
	}

	@Override
	public UUID getLocalUUID() {
		return id;
	}

}
