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
package com.paremus.gossip.net;

import java.net.InetSocketAddress;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paremus.gossip.Gossip;
import com.paremus.gossip.cluster.impl.MemberInfo;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public  class IncomingTCPReplicator extends AbstractTCPReplicator {

	private static final Logger logger = LoggerFactory.getLogger(IncomingTCPReplicator.class);
	
	public IncomingTCPReplicator(Channel channel, UUID localId, Gossip gossip) {
		super(channel, localId, gossip, gossip.getAllSnapshots());
	}

	@Override
	protected int validateExchangeHeader(ChannelHandlerContext ctx, long incomingExchangeId, UUID incomingId,
			int incomingSnapshotLength) {
		
		MemberInfo info = gossip.getInfoFor(incomingId);
		
		if(info != null && info.getAddress() != null) {
			InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
			
			if(!remoteAddress.getAddress().equals(info.getAddress())) {
				logger.warn("Received a synchronization request from address {} for node {}, but we think that node is at {}", remoteAddress.getAddress(), incomingId, info.getAddress());
				return -1;
			}
		}
		
		ctx.write(getHeader(ctx, incomingExchangeId));
		writeSnapshots(ctx, snapshotHeaders);
		
		return incomingSnapshotLength;
	}
}
