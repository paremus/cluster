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
import static com.paremus.gossip.v1.messages.MessageType.PING_REQUEST;

import io.netty.buffer.ByteBuf;

public class PingRequest extends AbstractGossipMessage {
	
	public PingRequest(String clusterName, Snapshot snapshot) {
		super(clusterName, snapshot);
	}
	
	public PingRequest(final ByteBuf input) {
		super(input);
	}
	
	public void writeOut(ByteBuf output) {
		super.writeOut(output);
	}
	
	@Override
	public MessageType getType() {
		return PING_REQUEST;
	}
}
