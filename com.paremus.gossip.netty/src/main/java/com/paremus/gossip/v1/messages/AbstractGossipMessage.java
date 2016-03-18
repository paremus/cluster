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
import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.InetSocketAddress;
import java.util.Objects;

import com.paremus.gossip.GossipMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;


public abstract class AbstractGossipMessage implements GossipMessage {
	
	private final String clusterName;
	
	private Snapshot update;
	
	public AbstractGossipMessage(String clusterName, Snapshot snapshot) {
		Objects.nonNull(snapshot);
		this.clusterName = clusterName;
		this.update = snapshot;
	}
	
	public AbstractGossipMessage(final ByteBuf input) {
		try {
			clusterName = input.readCharSequence(input.readUnsignedShort(), UTF_8).toString();
			update = new Snapshot(input);
		} catch (Exception e) {
			throw new RuntimeException("Failed to read message", e);
		}
	}
	
	public void writeOut(ByteBuf output) {
		try {
			writeUTF8(output, clusterName);
			update.writeOut(output);
		} catch (Exception e) {
			throw new RuntimeException("Failed to write message", e);
		}
	}

	public String getClusterName() {
		return clusterName;
	}
	
	public Snapshot getUpdate(InetSocketAddress sentFrom) {
		return new Snapshot(update, sentFrom);
	}
	
	public abstract MessageType getType();
	
	static void writeUTF8(ByteBuf buf, CharSequence charSequence) {
		int writerIndex = buf.writerIndex();
		buf.writerIndex(writerIndex + 2);
		int written = buf.writeCharSequence(charSequence, UTF_8);
		buf.setShort(writerIndex, written);
	}
	
	public int estimateSize() {
		return ByteBufUtil.utf8MaxBytes(clusterName) + update.guessSize();
	}
}
