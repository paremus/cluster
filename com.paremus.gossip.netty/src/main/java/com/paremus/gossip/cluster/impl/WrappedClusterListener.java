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

import static java.util.stream.Collectors.toSet;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paremus.cluster.ClusterInformation;
import com.paremus.cluster.listener.Action;
import com.paremus.cluster.listener.ClusterListener;

public class WrappedClusterListener implements ClusterListener {

	private static final Logger logger = LoggerFactory.getLogger(WrappedClusterListener.class);
	
	private final ClusterListener listener;
	
	private final Executor executor;
	
	private final AtomicReference<Function<Set<String>, Set<String>>> filter = new AtomicReference<>();
	
	public WrappedClusterListener(ClusterListener listener, Executor executor) {
		if(listener == null) throw new IllegalStateException("Listener is invalid");
		this.listener = listener;
		this.executor = executor;
		filter.set(Function.identity());
	}

	@Override
	public void clusterEvent(ClusterInformation ci, Action action, UUID id, Set<String> addedKeys,
			Set<String> removedKeys, Set<String> updatedKeys) {
		Function<Set<String>, Set<String>> f = filter.get();
		
		Set<String> filteredAdded = f.apply(addedKeys);
		Set<String> filteredRemoved = f.apply(removedKeys);
		Set<String> filteredUpdated = f.apply(updatedKeys);
		
		if(logger.isDebugEnabled()) {
			logger.debug("Event: {} {} {} {}", new Object[] {action, filteredAdded, filteredRemoved, filteredUpdated});
		}
		
		if(action == Action.UPDATED && addedKeys.isEmpty() && removedKeys.isEmpty() && updatedKeys.isEmpty()) {
			return;
		} else {
			executor.execute(() -> listener.clusterEvent(ci, action, id, filteredAdded, filteredRemoved, filteredUpdated));
		}
	}
	
	public void update(ServiceReference<ClusterListener> ref) {
		Set<String> wanted = getStringPlusProperty(ref, LIMIT_KEYS);
		
		if(wanted.isEmpty()) {
			filter.set(Function.identity());
		} else {
			filter.set((s) -> s.stream().filter(wanted::contains).collect(toSet()));
		}
		
	}

	static Set<String> getStringPlusProperty(ServiceReference<?> ref, String prop) {
		Set<String> wanted = new HashSet<>();
		
		Object o = ref.getProperty(prop);
		
		if(o instanceof String) {
			wanted.add(o.toString());
		} else if (o instanceof String[]) {
			wanted.addAll(Arrays.asList((String[]) o));
		} else if (o instanceof Collection) {
			((Collection<?>) o).forEach((s) -> wanted.add(s.toString()));
		}
		return wanted;
	}
}
