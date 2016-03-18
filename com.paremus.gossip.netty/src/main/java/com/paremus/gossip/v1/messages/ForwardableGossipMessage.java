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
package com.paremus.gossip.v1.messages;
import static com.paremus.gossip.v1.messages.MessageType.FORWARDABLE;
import static java.util.stream.Collectors.toList;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;


public class ForwardableGossipMessage extends AbstractGossipMessage {
	
	/**
	 * What are the other Snapshots from this message?
	 */
	private final List<Snapshot> forwards;
	
	public ForwardableGossipMessage(final ByteBuf input) {
		super(input);
		try {
			final int size = input.readUnsignedByte();
			
			forwards = new ArrayList<>(size);
			for(int i=0; i < size; i++) {
				forwards.add(new Snapshot(input));
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to read message", e);
		}
	}
	
	public ForwardableGossipMessage(String clusterName, Snapshot snapshot, List<Snapshot> forwards) {
		super(clusterName, snapshot);
		this.forwards = forwards;
	}	

	public void writeOut(ByteBuf output) {
		try {
			super.writeOut(output);
			output.writeByte(forwards.size());
		} catch (Exception e) {
			throw new RuntimeException("Failed to write snapshot", e);
		}
		
		forwards.forEach((s) -> s.writeOut(output));
	}

	public List<Snapshot> getAllSnapshots(InetSocketAddress sentFrom) {
		List<Snapshot> toReturn = forwards.stream().map(Snapshot::new).collect(toList());
		toReturn.add(getUpdate(sentFrom));
		return toReturn;
	}
	
	@Override
	public MessageType getType() {
		return FORWARDABLE;
	}

	@Override
	public int estimateSize() {
		return super.estimateSize() + 1 + forwards.stream()
			.mapToInt(Snapshot::guessSize)
			.sum();
	}
}
