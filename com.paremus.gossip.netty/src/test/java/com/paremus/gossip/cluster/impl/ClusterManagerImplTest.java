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
package com.paremus.gossip.cluster.impl;

import static com.paremus.cluster.listener.Action.ADDED;
import static com.paremus.cluster.listener.Action.REMOVED;
import static com.paremus.cluster.listener.Action.UPDATED;
import static com.paremus.gossip.v1.messages.SnapshotType.HEARTBEAT;
import static com.paremus.gossip.v1.messages.SnapshotType.PAYLOAD_UPDATE;
import static java.net.InetAddress.getLoopbackAddress;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.osgi.util.converter.Converters.standardConverter;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceReference;

import com.paremus.cluster.ClusterInformation;
import com.paremus.cluster.listener.Action;
import com.paremus.cluster.listener.ClusterListener;
import com.paremus.gossip.InternalClusterListener;
import com.paremus.gossip.netty.Config;
import com.paremus.gossip.v1.messages.Snapshot;
import com.paremus.gossip.v1.messages.SnapshotType;

import io.netty.util.concurrent.GlobalEventExecutor;

@RunWith(MockitoJUnitRunner.class)
public class ClusterManagerImplTest {

	static final String CLUSTER = "myCluster";
	static final String FOO = "foo";
	static final String BAR = "bar";
	static final String BAZ = "baz";
	static final byte[] PAYLOAD = {1, 2, 3, 4};
	static final byte[] PAYLOAD_2 = {5, 6, 7, 8};
	
	static final int UDP = 1234;
	static final int TCP = 1235;
	
	static final UUID ID = new UUID(1234, 5678);

	
	static final byte[] INCOMING_ADDRESS = {4, 3, 2, 1};
	static final int INCOMING_UDP = 2345;
	static final int INCOMING_TCP = 2346;
	
	static final UUID INCOMING_ID = new UUID(8765, 4321);
	
	@Mock
	BundleContext context;

	@Mock
	InternalClusterListener listener;
	
	ClusterManagerImpl impl;
	
	
	private void assertSnapshot(Snapshot s, int time, int sequence, int tcp, 
			UUID id, byte[] payload, SnapshotType type) {
		assertSnapshot(s, time, sequence, null, -1, tcp, id, payload, type);
	}

	private void assertSnapshot(Snapshot s, int time, int sequence, InetAddress address, int udp, int tcp, 
			UUID id, byte[] payload, SnapshotType type) {
		// Due to the 24 bit nature of the timestamp 1ms is actually 256 higher
		assertEquals("Snapshot should be the current time", time, s.getSnapshotTimestamp(), 257d);
		assertEquals("No data yet", sequence, s.getStateSequenceNumber());
		if(address == null) {
			assertNull("The address we send is always null", s.getAddress());
		} else {
			assertEquals(address, s.getAddress());
		}
		assertEquals("Wrong UDP port", udp, s.getUdpPort());
		assertEquals("Wrong TCP port", tcp, s.getTcpPort());
		assertEquals("Wrong id", id, s.getId());
		if(type == HEARTBEAT) {
			assertTrue("No payload for a heartbeat", s.getData().isEmpty());
		} else if (payload == null) {
			assertTrue("No payload expected", s.getData().isEmpty());
		} else {
			assertTrue("Wrong payload", Arrays.equals(payload, s.getData().get(FOO)));
		}
		assertEquals(type, s.getMessageType());
	}
	
	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		
		Mockito.when(listener.destroy()).thenReturn(GlobalEventExecutor.INSTANCE.newSucceededFuture(null));
		
		impl = new ClusterManagerImpl(context, ID, 
				standardConverter().convert(singletonMap("cluster.name", CLUSTER))
				.to(Config.class), UDP, TCP, null, x -> listener);
	}
	
	@Test
	public void testHeartbeatSnapshot() {
		int now = (int) (0xFFFFFF & NANOSECONDS.toMillis(System.nanoTime())) << 8;
		assertSnapshot(impl.getSnapshot(HEARTBEAT, 1), now, 0, TCP, ID, null, HEARTBEAT);
		
		impl.updateAttribute(FOO, PAYLOAD);
		
		now = (int) (0xFFFFFF & NANOSECONDS.toMillis(System.nanoTime())) << 8;
		assertSnapshot(impl.getSnapshot(HEARTBEAT, 1), now, 1, TCP, ID, null, HEARTBEAT);
	}

	@Test
	public void testUpdateSnapshot() {
		int now = (int) (0xFFFFFF & NANOSECONDS.toMillis(System.nanoTime())) << 8;
		assertSnapshot(impl.getSnapshot(PAYLOAD_UPDATE, 1), now, 0, TCP, ID, null, PAYLOAD_UPDATE);
		
		impl.updateAttribute(FOO, PAYLOAD);
		
		now = (int) (0xFFFFFF & NANOSECONDS.toMillis(System.nanoTime())) << 8;
		assertSnapshot(impl.getSnapshot(PAYLOAD_UPDATE, 1), now, 1, TCP, ID, PAYLOAD, PAYLOAD_UPDATE);
	}
	
	@Test
	public void testPayloadUpdateTriggersListener() {
		
		int now = (int) (0xFFFFFF & NANOSECONDS.toMillis(System.nanoTime())) << 8;

		impl.updateAttribute(FOO, PAYLOAD);
		
		ArgumentCaptor<Snapshot> captor = ArgumentCaptor.forClass(Snapshot.class);
		
		Mockito.verify(listener).localUpdate(captor.capture());
		
		assertSnapshot(captor.getValue(), now, 1, TCP, ID, PAYLOAD, PAYLOAD_UPDATE);
	}

	@Test
	public void testDestroyTriggersListener() {
		impl.destroy();
		
		Mockito.verify(listener).destroy();
	}
	
	@Test
	public void testBasicAddAndRemove() throws UnknownHostException {
		
		impl.mergeSnapshot(new Snapshot(impl.getSnapshot(PAYLOAD_UPDATE, 1), 
				new InetSocketAddress(InetAddress.getLocalHost(), 123)));
		
		assertTrue("Should know about ourselves", impl.getKnownMembers().contains(ID));
		assertFalse("Should not know about the new guy yet", impl.getKnownMembers().contains(INCOMING_ID));
		
		int now = (int) (0xFFFFFF & NANOSECONDS.toMillis(System.nanoTime())) << 8;
		impl.mergeSnapshot(new Snapshot(new Snapshot(INCOMING_ID, INCOMING_TCP, (short) 0, PAYLOAD_UPDATE, 
				Collections.singletonMap(FOO, PAYLOAD), 1), 
				new InetSocketAddress(InetAddress.getByAddress(INCOMING_ADDRESS), INCOMING_UDP)));
		
		assertTrue("Should now know about the new guy", impl.getKnownMembers().contains(INCOMING_ID));
		assertEquals("Should have the right address", InetAddress.getByAddress(INCOMING_ADDRESS), 
				impl.getAddressFor(INCOMING_ID));
		
		assertSnapshot(impl.getMemberInfo(INCOMING_ID).toSnapshot(), now, 0, InetAddress.getByAddress(INCOMING_ADDRESS), 
				INCOMING_UDP, INCOMING_TCP, INCOMING_ID, PAYLOAD, PAYLOAD_UPDATE);
		
		impl.leavingCluster(new Snapshot(INCOMING_ID, INCOMING_TCP, (short) 0, HEARTBEAT, null, 1));
		
		assertFalse("New guy should be gone now", impl.getKnownMembers().contains(INCOMING_ID));
		assertNull("Should have no address", impl.getAddressFor(INCOMING_ID));
	}
	
	@Test
	public void testMarkUnreachable() {
		MemberInfo info = Mockito.mock(MemberInfo.class);
		impl.markUnreachable(info);
		
		Mockito.verify(info).markUnreachable();
	}
	
	@Test
	public void testGetMemberAttributes() throws UnknownHostException {
		Map<String, byte[]> attrs = new HashMap<>();
		attrs.put(FOO, PAYLOAD);
		attrs.put(BAR, PAYLOAD_2);
		
		impl.mergeSnapshot(new Snapshot(new Snapshot(INCOMING_ID, INCOMING_TCP, (short) 0, PAYLOAD_UPDATE, 
				attrs, 1), new InetSocketAddress(InetAddress.getByAddress(INCOMING_ADDRESS), INCOMING_UDP)));
		
		Function<Map<String, byte[]>, Map<String, String>> equalsSafe = 
				m -> m.entrySet().stream()
					.collect(toMap(Entry::getKey, e -> Arrays.toString(e.getValue())));
		
		assertEquals(equalsSafe.apply(attrs), equalsSafe.apply(impl.getMemberAttributes(INCOMING_ID)));
		assertEquals(Arrays.toString(PAYLOAD), Arrays.toString(impl.getMemberAttribute(INCOMING_ID, FOO)));
		assertEquals(Arrays.toString(PAYLOAD_2), Arrays.toString(impl.getMemberAttribute(INCOMING_ID, BAR)));
		assertNull(impl.getMemberAttribute(INCOMING_ID, BAZ));
	}

	@Test
	public void testGetMemberSnapshots() throws UnknownHostException {
		//Just ourselves
		int now = (int) (0xFFFFFF & NANOSECONDS.toMillis(System.nanoTime())) << 8;
		impl.mergeSnapshot(new Snapshot(impl.getSnapshot(PAYLOAD_UPDATE, 0), new InetSocketAddress(getLoopbackAddress(), UDP)));
		
		Iterator<Snapshot> it = impl.getMemberSnapshots(PAYLOAD_UPDATE).iterator();
		assertSnapshot(it.next(), now, 0, getLoopbackAddress(), UDP, TCP, ID, null, PAYLOAD_UPDATE);
		assertFalse(it.hasNext());
		
		now = (int) (0xFFFFFF & NANOSECONDS.toMillis(System.nanoTime())) << 8;
		impl.updateAttribute(FOO, PAYLOAD);
		it = impl.getMemberSnapshots(PAYLOAD_UPDATE).iterator();
		
		assertSnapshot(it.next(), now, 1, getLoopbackAddress(), UDP, TCP, ID, PAYLOAD, PAYLOAD_UPDATE);
		assertFalse(it.hasNext());
		
		//Add a new member
		int now2 = (int) (0xFFFFFF & NANOSECONDS.toMillis(System.nanoTime())) << 8;
		impl.mergeSnapshot(new Snapshot(new Snapshot(INCOMING_ID, INCOMING_TCP, (short) 0, PAYLOAD_UPDATE, 
				Collections.singletonMap(FOO, PAYLOAD_2), 1), 
				new InetSocketAddress(InetAddress.getByAddress(INCOMING_ADDRESS), INCOMING_UDP)));
		
		it = impl.getMemberSnapshots(PAYLOAD_UPDATE).stream()
				.sorted((a,b) -> a.getId().compareTo(b.getId())).iterator();
		
		assertSnapshot(it.next(), now, 1, getLoopbackAddress(), UDP, TCP, ID, PAYLOAD, PAYLOAD_UPDATE);
		assertSnapshot(it.next(), now2, 0, InetAddress.getByAddress(INCOMING_ADDRESS), 
				INCOMING_UDP, INCOMING_TCP, INCOMING_ID, PAYLOAD_2, PAYLOAD_UPDATE);
		assertFalse(it.hasNext());
		
		
		impl.leavingCluster(new Snapshot(INCOMING_ID, INCOMING_TCP, (short) 0, HEARTBEAT, null, 1));
		
		it = impl.getMemberSnapshots(PAYLOAD_UPDATE).iterator();
		assertSnapshot(it.next(), now, 1, getLoopbackAddress(), UDP, TCP, ID, PAYLOAD, PAYLOAD_UPDATE);
		assertFalse(it.hasNext());
	}

	@Test
	public void testSelectRandomPartners() throws UnknownHostException {
		//Just ourselves
		impl.updateAttribute(FOO, PAYLOAD);
		
		Collection<MemberInfo> sample = impl.selectRandomPartners(2);
		
		assertTrue(sample.isEmpty());
		
		//Add a new member
		impl.mergeSnapshot(new Snapshot(new Snapshot(INCOMING_ID, INCOMING_TCP, (short) 0, PAYLOAD_UPDATE, 
				Collections.singletonMap(FOO, PAYLOAD_2), 1), 
				new InetSocketAddress(InetAddress.getByAddress(INCOMING_ADDRESS), INCOMING_UDP)));
		
		sample = impl.selectRandomPartners(2);
		
		assertEquals(1, sample.size());
		assertEquals(INCOMING_ID, sample.iterator().next().getId());
		
		impl.leavingCluster(new Snapshot(INCOMING_ID, INCOMING_TCP, (short) 0, HEARTBEAT, null, 1));
		
		sample = impl.selectRandomPartners(2);
		assertTrue(sample.isEmpty());
	}
	
	@Test
	public void testClusterListener() throws UnknownHostException, InterruptedException {
		
		impl.mergeSnapshot(new Snapshot(impl.getSnapshot(PAYLOAD_UPDATE, 0), new InetSocketAddress(getLoopbackAddress(), UDP)));
		
		Semaphore semA = new Semaphore(0);
		Semaphore semB = new Semaphore(0);
		
		ClusterListener listenerA = Mockito.mock(ClusterListener.class);
		@SuppressWarnings("unchecked")
		ServiceReference<ClusterListener> refA = Mockito.mock(ServiceReference.class);
		Mockito.when(context.getService(refA)).thenReturn(listenerA);
		Mockito.doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				semA.release();
				return null;
			}
		}).when(listenerA).clusterEvent(any(ClusterInformation.class), any(Action.class), 
				any(UUID.class), anySet(), anySet(), anySet());
		
		ClusterListener listenerB = Mockito.mock(ClusterListener.class);
		@SuppressWarnings("unchecked")
		ServiceReference<ClusterListener> refB = Mockito.mock(ServiceReference.class);
		Mockito.when(refB.getProperty(ClusterListener.CLUSTER_NAMES)).thenReturn(Collections.singleton(CLUSTER));
		Mockito.when(context.getService(refB)).thenReturn(listenerB);
		Mockito.doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				semB.release();
				return null;
			}
		}).when(listenerB).clusterEvent(any(ClusterInformation.class), any(Action.class), 
				any(UUID.class), anySet(), anySet(), anySet());
		
		@SuppressWarnings("unchecked")
		ServiceReference<ClusterListener> refC = Mockito.mock(ServiceReference.class);
		Mockito.when(refC.getProperty(ClusterListener.CLUSTER_NAMES)).thenReturn(Collections.singleton(CLUSTER + "2"));

		impl.listenerChange(refA, ServiceEvent.REGISTERED);
		assertTrue(semA.tryAcquire(1000, TimeUnit.MILLISECONDS));
		Mockito.verify(listenerA).clusterEvent(impl, ADDED, ID, emptySet(), emptySet(), emptySet());
		
		impl.listenerChange(refB, ServiceEvent.REGISTERED);
		assertTrue(semB.tryAcquire(1000, TimeUnit.MILLISECONDS));
		Mockito.verify(listenerB).clusterEvent(impl, ADDED, ID, emptySet(), emptySet(), emptySet());

		impl.listenerChange(refC, ServiceEvent.REGISTERED);

		impl.updateAttribute(FOO, PAYLOAD);
		assertTrue(semA.tryAcquire(1000, TimeUnit.MILLISECONDS));
		assertTrue(semB.tryAcquire(1000, TimeUnit.MILLISECONDS));
		
		Mockito.verify(listenerA).clusterEvent(impl, UPDATED, ID, singleton(FOO), emptySet(), emptySet());
		Mockito.verify(listenerB).clusterEvent(impl, UPDATED, ID, singleton(FOO), emptySet(), emptySet());
		
		//Sleep to make sure the next snapshot isn't ignored
		Thread.sleep(20);
		
		impl.updateAttribute(FOO, PAYLOAD_2);
		
		assertTrue(semA.tryAcquire(1000, TimeUnit.MILLISECONDS));
		assertTrue(semB.tryAcquire(1000, TimeUnit.MILLISECONDS));
		
		Mockito.verify(listenerA).clusterEvent(impl, UPDATED, ID, emptySet(), emptySet(), singleton(FOO));
		Mockito.verify(listenerB).clusterEvent(impl, UPDATED, ID, emptySet(), emptySet(), singleton(FOO));
		
		//Add a new member
		Map<String, byte[]> attrs = new HashMap<>();
		attrs.put(FOO, PAYLOAD);
		attrs.put(BAR, PAYLOAD_2);
		
		//Sleep to make sure the next snapshot isn't ignored
		Thread.sleep(20);
		impl.mergeSnapshot(new Snapshot(new Snapshot(INCOMING_ID, INCOMING_TCP, (short) 0, PAYLOAD_UPDATE, 
				attrs, 1), new InetSocketAddress(InetAddress.getByAddress(INCOMING_ADDRESS), INCOMING_UDP)));
		
		assertTrue(semA.tryAcquire(1000, TimeUnit.MILLISECONDS));
		assertTrue(semB.tryAcquire(1000, TimeUnit.MILLISECONDS));
		
		Mockito.verify(listenerA).clusterEvent(impl, ADDED, INCOMING_ID, new HashSet<>(Arrays.asList(FOO, BAR)), 
				emptySet(), emptySet());
		Mockito.verify(listenerB).clusterEvent(impl, ADDED, INCOMING_ID, new HashSet<>(Arrays.asList(FOO, BAR)), 
				emptySet(), emptySet());

		
		//Update the member
		attrs.remove(FOO);
		attrs.put(BAR, PAYLOAD);
		attrs.put(BAZ, PAYLOAD_2);
		
		//Sleep to make sure the next snapshot isn't ignored
		Thread.sleep(20);
		impl.mergeSnapshot(new Snapshot(new Snapshot(INCOMING_ID, INCOMING_TCP, (short) 1, PAYLOAD_UPDATE, 
				attrs, 1), new InetSocketAddress(InetAddress.getByAddress(INCOMING_ADDRESS), INCOMING_UDP)));
		
		assertTrue(semA.tryAcquire(500000, TimeUnit.MILLISECONDS));
		assertTrue(semB.tryAcquire(1000, TimeUnit.MILLISECONDS));
		
		Mockito.verify(listenerA).clusterEvent(impl, UPDATED, INCOMING_ID, singleton(BAZ), singleton(FOO), singleton(BAR));
		Mockito.verify(listenerB).clusterEvent(impl, UPDATED, INCOMING_ID, singleton(BAZ), singleton(FOO), singleton(BAR));
		
		//Sleep to make sure the next snapshot isn't ignored
		Thread.sleep(20);
		impl.leavingCluster(new Snapshot(INCOMING_ID, INCOMING_TCP, (short) 0, HEARTBEAT, null, 1));
		
		assertTrue(semA.tryAcquire(1000, TimeUnit.MILLISECONDS));
		assertTrue(semB.tryAcquire(1000, TimeUnit.MILLISECONDS));
		
		Mockito.verify(listenerA).clusterEvent(impl, REMOVED, INCOMING_ID, emptySet(), 
				new HashSet<>(Arrays.asList(BAR, BAZ)), emptySet());
		Mockito.verify(listenerB).clusterEvent(impl, REMOVED, INCOMING_ID, emptySet(), 
				new HashSet<>(Arrays.asList(BAR, BAZ)), emptySet());
		
		Mockito.verify(context, Mockito.never()).getService(refC);
	}
}
