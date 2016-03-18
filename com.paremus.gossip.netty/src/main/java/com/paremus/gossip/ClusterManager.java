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
package com.paremus.gossip;

import java.util.Collection;
import java.util.UUID;

import org.osgi.framework.ServiceReference;

import com.paremus.cluster.listener.ClusterListener;
import com.paremus.gossip.cluster.impl.MemberInfo;
import com.paremus.gossip.cluster.impl.Update;
import com.paremus.gossip.v1.messages.Snapshot;
import com.paremus.gossip.v1.messages.SnapshotType;

import io.netty.util.concurrent.EventExecutorGroup;

public interface ClusterManager {

	public void leavingCluster(Snapshot update);

	public String getClusterName();
	
	public Update mergeSnapshot(Snapshot snapshot);

	public Snapshot getSnapshot(SnapshotType type, int hops);

	public Collection<MemberInfo> selectRandomPartners(int max);

	public MemberInfo getMemberInfo(UUID id);

	public Collection<Snapshot> getMemberSnapshots(SnapshotType type);

	public void markUnreachable(MemberInfo member);

	public void destroy();
	
	public UUID getLocalUUID();
	
	public void listenerChange(ServiceReference<ClusterListener> ref, int state);
	
	public EventExecutorGroup getEventExecutorGroup();
}
