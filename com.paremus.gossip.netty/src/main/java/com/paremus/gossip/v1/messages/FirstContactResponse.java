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
import static com.paremus.gossip.v1.messages.MessageType.FIRST_CONTACT_RESPONSE;

import java.util.Objects;

import io.netty.buffer.ByteBuf;


public class FirstContactResponse extends AbstractGossipMessage {
	
	private final Snapshot firstContactInfo;
	
	public FirstContactResponse(String clusterName, Snapshot snapshot, Snapshot receivedFrom) {
		super(clusterName, snapshot);
		Objects.nonNull(receivedFrom);
		this.firstContactInfo = receivedFrom;
	}
	
	public FirstContactResponse(final ByteBuf input) {
		super(input);
		firstContactInfo = new Snapshot(input);
	}
	
	public void writeOut(ByteBuf output) {
		super.writeOut(output);
		firstContactInfo.writeOut(output);
	}

	public Snapshot getFirstContactInfo() {
		return firstContactInfo;
	}

	@Override
	public MessageType getType() {
		return FIRST_CONTACT_RESPONSE;
	}
	
	@Override
	public int estimateSize() {
		return super.estimateSize() + firstContactInfo.guessSize();
	}
}
