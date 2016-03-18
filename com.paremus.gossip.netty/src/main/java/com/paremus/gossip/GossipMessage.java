/*-
 * #%L
 * The Paremus Gossip Clustering (Netty) Implementation
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
package com.paremus.gossip;

import com.paremus.gossip.v1.messages.MessageType;

import io.netty.buffer.ByteBuf;

public interface GossipMessage {
	
	public void writeOut(ByteBuf output);

	public abstract MessageType getType();
	
	public int estimateSize();

}
