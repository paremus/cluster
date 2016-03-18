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

import java.util.Collection;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paremus.gossip.Gossip;
import com.paremus.gossip.v1.messages.Snapshot;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;

public class OutgoingTCPReplicator extends AbstractTCPReplicator {

	private static final Logger logger = LoggerFactory.getLogger(OutgoingTCPReplicator.class);
	
	private final UUID remoteId;
	
	private final long exchangeId;
	
	private final Future<?> start;
	
	public OutgoingTCPReplicator(Channel channel, UUID localId, Gossip gossip, UUID remoteId, long exchangeId, 
			Collection<Snapshot> snapshotHeaders, Future<?> readyToSend) {
		super(channel, localId, gossip, snapshotHeaders);
		this.remoteId = remoteId;
		this.exchangeId = exchangeId;
		this.start = readyToSend;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {

		start.addListener(f -> {
			if(f.isSuccess()) {
				ctx.write(getHeader(ctx, exchangeId));
				writeSnapshots(ctx, snapshotHeaders);
			} else {
				if(logger.isDebugEnabled()) {
					logger.debug("Failed to synchronize with {} on {}", remoteId, ctx.channel().remoteAddress(), f.cause());
				}
				ctx.close();
			}
		});

		super.channelActive(ctx);
	}

	protected int validateExchangeHeader(ChannelHandlerContext ctx, long incomingExchangeId, UUID incomingId,
			int incomingSnapshotLength) {
		if(incomingExchangeId != exchangeId) {
			logger.warn("The TCP cluster exchange with " + ctx.channel().remoteAddress() + " responded with an invalid id");
			return -1;
		}
		if(remoteId != null) {
			if(!remoteId.equals(incomingId)) {
				logger.warn("The TCP cluster exchange with " + ctx.channel().remoteAddress() + " responded with an unexpected id " + incomingId + " not " + remoteId);
				return -1;
			}
		} else if(logger.isDebugEnabled()){
			logger.debug("Participating in exchange " + exchangeId + " with " + localId);
		}
		return incomingSnapshotLength;
	}
}
