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

import java.util.function.Function;

import io.netty.buffer.ByteBuf;

public enum MessageType {
	FIRST_CONTACT_REQUEST(FirstContactRequest::new), 
	FIRST_CONTACT_RESPONSE(FirstContactResponse::new), 
	FORWARDABLE(ForwardableGossipMessage::new), 
	DISCONNECTION(DisconnectionMessage::new), 
	PING_REQUEST(PingRequest::new), 
	PING_RESPONSE(PingResponse::new);
	
	private final Function<ByteBuf, AbstractGossipMessage> creator;
	
	private MessageType(Function<ByteBuf, AbstractGossipMessage> creator) {
		this.creator = creator;
	}
	
	public AbstractGossipMessage fromBuffer(ByteBuf buf) {
		return creator.apply(buf);
	}
}
