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

import static com.paremus.gossip.cluster.impl.Update.FORWARD;
import static com.paremus.gossip.cluster.impl.Update.FORWARD_LOCAL;
import static com.paremus.gossip.v1.messages.MessageType.DISCONNECTION;
import static com.paremus.gossip.v1.messages.MessageType.FIRST_CONTACT_REQUEST;
import static com.paremus.gossip.v1.messages.SnapshotType.PAYLOAD_UPDATE;
import static java.net.InetAddress.getByAddress;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.osgi.util.converter.Converters.standardConverter;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.osgi.util.promise.Promise;

import com.paremus.gossip.ClusterManager;
import com.paremus.gossip.GossipComms;
import com.paremus.gossip.GossipMessage;
import com.paremus.gossip.InternalClusterListener;
import com.paremus.gossip.cluster.impl.MemberInfo;
import com.paremus.gossip.netty.Config;
import com.paremus.gossip.v1.messages.DisconnectionMessage;
import com.paremus.gossip.v1.messages.FirstContactRequest;
import com.paremus.gossip.v1.messages.FirstContactResponse;
import com.paremus.gossip.v1.messages.ForwardableGossipMessage;
import com.paremus.gossip.v1.messages.MessageType;
import com.paremus.gossip.v1.messages.PingRequest;
import com.paremus.gossip.v1.messages.PingResponse;
import com.paremus.gossip.v1.messages.Snapshot;
import com.paremus.gossip.v1.messages.SnapshotType;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

@RunWith(MockitoJUnitRunner.class)
public class GossipImplTest {

	static final byte[] INCOMING_ADDRESS = {4, 3, 2, 1};
	
	static final String CLUSTER = "cluster";
	static final String ANOTHER_CLUSTER = "another-cluster";
	
	static final String FOO = "foo";
	static final String BAR = "bar";
	static final String BAZ = "baz";
	static final byte[] PAYLOAD = {1, 2, 3, 4};
	static final byte[] PAYLOAD_2 = {5, 6, 7, 8};
	
	static final int UDP = 1234;
	static final int TCP = 1235;

	static final int UDP_2 = 2345;
	static final int TCP_2 = 2346;
	
	static final UUID LOCAL_ID = new UUID(1, 2);
	static final UUID ID = new UUID(1234, 5678);
	static final UUID ID_2 = new UUID(2345, 6789);
	static final UUID ID_3 = new UUID(3456, 7890);
	
	@Mock
	ClusterManager mgr;

	@Mock
	GossipComms comms;

	InetSocketAddress sockA = new InetSocketAddress(9876);

	InetSocketAddress sockB = new InetSocketAddress(8765);

	@Mock
	MemberInfo info;

	@SuppressWarnings("rawtypes")
	@Mock
	Promise promise;

	@Mock
	BundleContext context;

	private EventExecutorGroup executorGroup;
	
	@Before
	public void setUp() throws Exception {
		
		executorGroup = new DefaultEventExecutorGroup(1);
		
		Mockito.when(mgr.getClusterName()).thenReturn(CLUSTER);
		Mockito.when(mgr.getLocalUUID()).thenReturn(LOCAL_ID);
		Mockito.when(mgr.getSnapshot(Mockito.any(SnapshotType.class), Mockito.anyInt())).thenAnswer((i) ->
			new Snapshot(ID_2, TCP_2, (short) 0, (SnapshotType) i.getArguments()[0], singletonMap(BAR, PAYLOAD_2), 1));
		
		Mockito.when(mgr.getEventExecutorGroup()).thenReturn(executorGroup);
	}
	
	@After
	public void stop() throws InterruptedException {
		executorGroup.shutdownGracefully(100, 500, TimeUnit.MILLISECONDS).sync();
	}

	private GossipImpl getGossipImpl() throws Exception {
		Config config;
		try {
			config = standardConverter().convert(singletonMap("cluster.name", CLUSTER)).to(Config.class);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return new GossipImpl(context, mgr, x -> comms, config, Arrays.asList(sockA, sockB));
	}

	@Test
	public void testHandleFirstContactMessage() throws Exception {
		GossipImpl gossip = getGossipImpl();
		InetSocketAddress incoming = new InetSocketAddress(getByAddress(INCOMING_ADDRESS), UDP);
		gossip.handleMessage(incoming, new FirstContactRequest(CLUSTER, 
						new Snapshot(ID, TCP, (short) 0, PAYLOAD_UPDATE, singletonMap(FOO, PAYLOAD), 1)));
		gossip.destroy();
		
		ArgumentCaptor<GossipMessage> captor = ArgumentCaptor.forClass(GossipMessage.class);
		
		verify(comms).publish(captor.capture(), 
				eq(Collections.singleton(incoming)));
		
		GossipMessage msg = captor.getValue();
		
		assertEquals(MessageType.FIRST_CONTACT_RESPONSE, msg.getType());
		FirstContactResponse fcr = (FirstContactResponse) msg;
		
		assertEquals(CLUSTER, fcr.getClusterName());
		assertEquals(incoming, fcr.getFirstContactInfo().getUdpAddress());
	}
	
	@Test
	public void testHandleFirstContactResponse() throws Exception {
		
		InetSocketAddress incoming = new InetSocketAddress(getByAddress(INCOMING_ADDRESS), UDP);
		Mockito.when(mgr.getMemberInfo(Mockito.eq(ID_2))).thenReturn(info);
		
		GossipImpl gossip = getGossipImpl();
		Snapshot original = new Snapshot(new Snapshot(ID, TCP, (short) 0, PAYLOAD_UPDATE, singletonMap(FOO, PAYLOAD), 1), 
				new InetSocketAddress(InetAddress.getLoopbackAddress(), UDP));
		gossip.handleMessage(incoming, new FirstContactResponse(CLUSTER, 
				mgr.getSnapshot(PAYLOAD_UPDATE, 1), original));
		
		gossip.destroy();
		
		verify(comms).replicate(Mockito.same(info), Mockito.anyCollection());
	}

	@Test
	public void testHandleForwardableHeartbeat() throws Exception {
		
		InetSocketAddress incoming = new InetSocketAddress(getByAddress(INCOMING_ADDRESS), UDP);
		Mockito.when(mgr.mergeSnapshot(Mockito.any(Snapshot.class))).thenReturn(FORWARD);
		Mockito.when(mgr.selectRandomPartners(Mockito.anyInt())).thenReturn(singletonList(info));
		Mockito.when(info.getUdpAddress()).thenReturn(new InetSocketAddress(UDP));
		
		Semaphore s = new Semaphore(0);
		Mockito.doAnswer((i) -> { s.release(); return null; }).when(comms)
			.publish(Mockito.any(GossipMessage.class), Mockito.any());
		
		GossipImpl gossip = getGossipImpl();
		
		gossip.handleMessage(incoming, new ForwardableGossipMessage(CLUSTER, 
				new Snapshot(ID, TCP, (short) 0, PAYLOAD_UPDATE, singletonMap(FOO, PAYLOAD), 3), 
				singletonList(new Snapshot(ID_3, TCP, (short) 0, PAYLOAD_UPDATE, singletonMap(BAR, PAYLOAD_2), 1))));
		
		assertTrue(s.tryAcquire(2, 1000, TimeUnit.MILLISECONDS));
		
		gossip.destroy();
		
		ArgumentCaptor<GossipMessage> captor = ArgumentCaptor.forClass(GossipMessage.class);
		
		verify(comms, Mockito.atLeastOnce()).publish(captor.capture(), Mockito.anyCollection());
		
		for(GossipMessage msg : captor.getAllValues()) {
			
			MessageType messageType = msg.getType();
			if(FIRST_CONTACT_REQUEST == messageType ||
					DISCONNECTION == messageType) continue;
			
			assertEquals(MessageType.FORWARDABLE, messageType);
			ForwardableGossipMessage fgm = (ForwardableGossipMessage) msg;
			
			assertEquals(CLUSTER, fgm.getClusterName());
			assertEquals(ID_2, fgm.getUpdate(incoming).getId());
			// This if block catches any gossip messages sent while we were waiting, and after the first peer arrived, as well as the forward
			List<Snapshot> allSnapshots = fgm.getAllSnapshots(incoming);
			if(allSnapshots.size() > 1) {
				assertEquals(2, allSnapshots.size());
				assertEquals(ID, allSnapshots.get(0).getId());
				assertEquals(ID_2, allSnapshots.get(1).getId());
			} else {
				assertEquals(ID_2, allSnapshots.get(0).getId());
			}
		}
	}

	@Test
	public void testHandleForwardableHeartbeatForwardLocal() throws Exception {
		
		InetSocketAddress incoming = new InetSocketAddress(getByAddress(INCOMING_ADDRESS), UDP);
		Mockito.when(mgr.mergeSnapshot(Mockito.any(Snapshot.class))).thenReturn(FORWARD_LOCAL);
		Mockito.when(mgr.selectRandomPartners(Mockito.anyInt())).thenReturn(singletonList(info));
		
		Snapshot a = new Snapshot(ID, new InetSocketAddress(InetAddress.getLocalHost(), 12), 
				TCP, (short) 1, 0xB00B00, PAYLOAD_UPDATE, singletonMap(FOO, PAYLOAD_2), 2);
		MemberInfo m = Mockito.mock(MemberInfo.class);
		Mockito.when(mgr.getMemberInfo(ID)).thenReturn(m);
		Mockito.when(m.toSnapshot(PAYLOAD_UPDATE, 2)).thenReturn(a);
		
		Mockito.when(info.getUdpAddress()).thenReturn(new InetSocketAddress(UDP));
		
		Semaphore s = new Semaphore(0);
		Mockito.doAnswer((i) -> { s.release(); return null; }).when(comms)
			.publish(Mockito.any(GossipMessage.class), Mockito.any());
		
		GossipImpl gossip = getGossipImpl();
		
		gossip.handleMessage(incoming, new ForwardableGossipMessage(CLUSTER, 
				new Snapshot(ID, TCP, (short) 0, PAYLOAD_UPDATE, singletonMap(FOO, PAYLOAD), 3), 
				singletonList(new Snapshot(ID_3, TCP, (short) 0, PAYLOAD_UPDATE, singletonMap(BAR, PAYLOAD_2), 1))));
		
		assertTrue(s.tryAcquire(2, 1000, TimeUnit.MILLISECONDS));
		
		gossip.destroy();
		
		ArgumentCaptor<GossipMessage> captor = ArgumentCaptor.forClass(GossipMessage.class);
		
		verify(comms, Mockito.atLeastOnce()).publish(captor.capture(), Mockito.anyCollection());
		
		for(GossipMessage msg : captor.getAllValues()) {
			
			MessageType messageType = msg.getType();
			if(FIRST_CONTACT_REQUEST == messageType ||
					DISCONNECTION == messageType) continue;
			
			assertEquals(MessageType.FORWARDABLE, messageType);
			ForwardableGossipMessage fgm = (ForwardableGossipMessage) msg;
			
			assertEquals(CLUSTER, fgm.getClusterName());
			Snapshot update = fgm.getUpdate(incoming);
			assertEquals(ID_2, update.getId());
			// This if block catches any gossip messages sent while we were waiting, and after the first peer arrived, as well as the forward
			List<Snapshot> allSnapshots = fgm.getAllSnapshots(incoming);
			if(allSnapshots.size() > 1) {
				assertEquals(2, allSnapshots.size());
				assertEquals(ID, allSnapshots.get(0).getId());
				assertEquals(a.getSnapshotTimestamp(), allSnapshots.get(0).getSnapshotTimestamp());
				assertEquals(ID_2, allSnapshots.get(1).getId());
			} else {
				assertEquals(ID_2, allSnapshots.get(0).getId());
			}
		}
	}
	
	@Test
	public void testHandleDisconnection() throws Exception {
		
		InetSocketAddress incoming = new InetSocketAddress(getByAddress(INCOMING_ADDRESS), UDP);
		GossipImpl gossip = getGossipImpl();
		
		gossip.handleMessage(incoming, new DisconnectionMessage(CLUSTER, 
				new Snapshot(ID, TCP, (short) 0, PAYLOAD_UPDATE, singletonMap(FOO, PAYLOAD), 1)));
		
		gossip.destroy();
		
		verify(mgr).leavingCluster(Mockito.argThat(hasSnapshotWith(ID)));
	}

	@Test
	public void testHandlePingRequest() throws Exception {
		
		InetSocketAddress incoming = new InetSocketAddress(getByAddress(INCOMING_ADDRESS), UDP);
		GossipImpl gossip = getGossipImpl();
		
		gossip.handleMessage(incoming, new PingRequest(CLUSTER, 
				new Snapshot(ID, TCP, (short) 0, PAYLOAD_UPDATE, singletonMap(FOO, PAYLOAD), 1)));
		
		gossip.destroy();
		
		ArgumentCaptor<GossipMessage> captor = ArgumentCaptor.forClass(GossipMessage.class);
		
		Mockito.verify(comms).publish(captor.capture(), Mockito.eq(Collections.singleton(incoming)));
		
		assertEquals(MessageType.PING_RESPONSE, captor.getValue().getType());
	}

	@Test
	public void testHandlePingResponse() throws Exception {
		
		InetSocketAddress incoming = new InetSocketAddress(getByAddress(INCOMING_ADDRESS), UDP);
		GossipImpl gossip = getGossipImpl();
		
		gossip.handleMessage(incoming, new PingResponse(CLUSTER, 
				new Snapshot(ID, TCP, (short) 0, PAYLOAD_UPDATE, singletonMap(FOO, PAYLOAD), 1)));
		
		gossip.destroy();
		
		verify(mgr).mergeSnapshot(Mockito.argThat(hasSnapshotWith(ID)));
	}

	private static ArgumentMatcher<Snapshot> hasSnapshotWith(UUID id) {
		return new ArgumentMatcher<Snapshot>() {
			
			@Override
			public boolean matches(Snapshot item) {
				return id.equals(item.getId());
			}
		};
	}

	@Test
	public void testDestroy() throws Exception {
		InetSocketAddress incoming = new InetSocketAddress(getByAddress(INCOMING_ADDRESS), UDP);
		Snapshot a = Mockito.mock(Snapshot.class);
		Mockito.when(a.getUdpAddress()).thenReturn(incoming);
		Mockito.when(mgr.getMemberSnapshots(Mockito.any(SnapshotType.class))).thenReturn(Arrays.asList(a));
		
		InternalClusterListener gossip = getGossipImpl();
		gossip.destroy();
		
		ArgumentCaptor<GossipMessage> captor = ArgumentCaptor.forClass(GossipMessage.class);
		
		verify(comms, Mockito.atLeastOnce()).publish(captor.capture(), 
				eq(Collections.singletonList(incoming)));
		
		GossipMessage msg = captor.getValue();
		
		assertEquals(MessageType.DISCONNECTION, msg.getType());
		DisconnectionMessage dm = (DisconnectionMessage) msg;
		
		assertEquals(CLUSTER, dm.getClusterName());
		assertEquals(ID_2, dm.getUpdate(incoming).getId());
		
	}

	@Test
	public void testDarkNodes() throws Exception {
		MemberInfo m = Mockito.mock(MemberInfo.class);
		Mockito.when(m.getUdpAddress()).thenReturn(new InetSocketAddress(UDP));
		InternalClusterListener gossip = getGossipImpl();
		gossip.darkNodes(Arrays.asList(m));
		gossip.destroy();
		
		ArgumentCaptor<GossipMessage> captor = ArgumentCaptor.forClass(GossipMessage.class);
		
		Mockito.verify(comms).publish(captor.capture(), Mockito.eq(Collections.singleton(new InetSocketAddress(UDP))));
		
		assertEquals(MessageType.PING_REQUEST, captor.getValue().getType());
	}
}
