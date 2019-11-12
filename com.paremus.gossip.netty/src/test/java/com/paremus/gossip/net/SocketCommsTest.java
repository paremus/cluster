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

import static com.paremus.gossip.v1.messages.SnapshotType.PAYLOAD_UPDATE;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static java.net.InetAddress.getLoopbackAddress;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.osgi.util.converter.Converters.standardConverter;

import java.net.BindException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;

import com.paremus.gossip.Gossip;
import com.paremus.gossip.GossipMessage;
import com.paremus.gossip.cluster.impl.MemberInfo;
import com.paremus.gossip.netty.Config;
import com.paremus.gossip.v1.messages.AbstractGossipMessage;
import com.paremus.gossip.v1.messages.FirstContactRequest;
import com.paremus.gossip.v1.messages.MessageType;
import com.paremus.gossip.v1.messages.Snapshot;
import com.paremus.netty.tls.ParemusNettyTLS;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@RunWith(MockitoJUnitRunner.class)
public class SocketCommsTest extends AbstractLeakCheckingTest {

	private static final int TCP_PORT = 18246;

	private static final int UDP_PORT = 18245;

	static final UUID ID = new UUID(1234, 5678);
	
	NettyComms socketComms;
	
	Map<String, Object> config = new HashMap<>();
	
	Semaphore sem = new Semaphore(0);
	
	ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
	
	@Mock
	Gossip gossip;

	@Mock
	MemberInfo member;

	@Mock
	ParemusNettyTLS tls;

	@Before
	public void setUp() throws Exception {
		config.put("udp.port", UDP_PORT);
		config.put("tcp.port", TCP_PORT);
		
		MockitoAnnotations.initMocks(this);
		socketComms = new NettyComms("cluster", ID, standardConverter()
				.convert(config).to(Config.class), tls, gossip);
		
		Mockito.doAnswer(this::count).when(gossip)
			.handleMessage(Mockito.any(InetSocketAddress.class), Mockito.any(GossipMessage.class));
	}
	
	private Object count(InvocationOnMock invocation) {
		sem.release();
		return null;
	}
	
	@After
	public void tearDown() throws Exception {
		socketComms.destroy();
	}

	@Test
	public void testStartListeningPlain() throws Exception {
		AbstractGossipMessage gm = getTestPacket();
		try (DatagramSocket s = new DatagramSocket()) {
			byte[] bytes = new byte[1024];
			
			ByteBuf buffer = Unpooled.wrappedBuffer(bytes);
			buffer.readerIndex(0);
			buffer.writerIndex(0);
			
			buffer.writeByte(2);
			buffer.writeByte(1);
			buffer.writeByte(gm.getType().ordinal());
			gm.writeOut(buffer);
			
			s.send(new DatagramPacket(bytes, buffer.readableBytes(), getLoopbackAddress(), 
					UDP_PORT));
			
			assertTrue(sem.tryAcquire(1000, MILLISECONDS));
			
			ArgumentCaptor<GossipMessage> captor = ArgumentCaptor.forClass(GossipMessage.class);
			Mockito.verify(gossip).handleMessage(Mockito.eq(new InetSocketAddress(getLoopbackAddress(), 
					s.getLocalPort())), captor.capture());
			validateSnapshot(((AbstractGossipMessage)captor.getValue())
					.getUpdate((InetSocketAddress)s.getRemoteSocketAddress()));
		}
	}
	
	@Test
	public void testStopListening() throws Exception {
		socketComms.destroy();
		
		try (DatagramSocket s = new DatagramSocket()) {
			s.send(new DatagramPacket(new byte[] {2, 1, 1, 0x7F}, 4, getLoopbackAddress(), 
					UDP_PORT));
		}
		//Should be no messages heard
		assertFalse(sem.tryAcquire(1000, MILLISECONDS));
	}

	@Test
	public void testDestroy() throws SocketException, InterruptedException {
		try (DatagramSocket d = new DatagramSocket(UDP_PORT)) {
			fail("Should be prevented from binding");
		} catch (BindException be) {}
		
		socketComms.destroy().sync();
		
		try (DatagramSocket d = new DatagramSocket(UDP_PORT)) {}
		
	}

	@Test
	public void testPublish() throws Exception {
		GossipMessage gm = getTestPacket();
		try (DatagramSocket s = new DatagramSocket(0, getLoopbackAddress())) {
			socketComms.publish(gm, Arrays.asList((InetSocketAddress)s.getLocalSocketAddress()));
			s.setSoTimeout(500);
			DatagramPacket packet = new DatagramPacket(new byte[1024], 1024);
			s.receive(packet);
			
			validatePacket(s, packet);
		}
	}

	private AbstractGossipMessage getTestPacket() {
		AbstractGossipMessage gm = new FirstContactRequest("FOO", 
				new Snapshot(ID, 1234, (short) 0, PAYLOAD_UPDATE, singletonMap("FOO", new byte[] {1,2,3,4}), 1));
		return gm;
	}

	private void validatePacket(DatagramSocket s, DatagramPacket packet) {
		assertEquals(2, packet.getData()[packet.getOffset()]);
		assertEquals(1, packet.getData()[packet.getOffset() + 1]);
		assertEquals(MessageType.FIRST_CONTACT_REQUEST.ordinal(), packet.getData()[packet.getOffset() + 2]);
		
		ByteBuf b = wrappedBuffer(packet.getData(), packet.getOffset() + 3, 
				packet.getLength() - 3);
		
		FirstContactRequest request = new FirstContactRequest(b);
		Snapshot sent = request.getUpdate((InetSocketAddress) s.getLocalSocketAddress());
		
		validateSnapshot(sent);
	}

	private void validateSnapshot(Snapshot sent) {
		assertEquals(ID, sent.getId());
		assertEquals(1234, sent.getTcpPort());
		assertEquals(0, sent.getStateSequenceNumber());
		assertEquals(PAYLOAD_UPDATE, sent.getMessageType());
		assertArrayEquals(new byte[] {1,2,3,4}, sent.getData().get("FOO"));
		assertEquals(0, sent.getRemainingHops());
	}
}
