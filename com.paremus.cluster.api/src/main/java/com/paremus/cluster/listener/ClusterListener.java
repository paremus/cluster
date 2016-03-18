/*-
 * #%L
 * com.paremus.cluster.api
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
package com.paremus.cluster.listener;

import java.util.Set;
import java.util.UUID;

import org.osgi.annotation.versioning.ConsumerType;

import com.paremus.cluster.ClusterInformation;

/**
 * A whiteboard service interface implemented by bundles who wish to be notified
 * about changes in the cluster
 *
 * <p>
 * The {@link #CLUSTER_NAMES} service property can be used to limit the set of clusters
 * for which this service will be called back. If it is not set then the service will be
 * notified for <strong>all</strong> clusters
 * 
 * <p>
 * The {@link #LIMIT_KEYS} service property can be used to limit the set of property
 * keys in which this service is interested. If it is not set then the service will
 * be notified of changes to <strong>all</strong> changes in stored values.
 */
@ConsumerType
public interface ClusterListener {
	
	/**
	 * This service property is used to limit the set of key/value changes the service
	 * will be notified about
	 */
	public static final String LIMIT_KEYS = "limit.keys";

	/**
	 * This service property is used to limit the set of clusters that the service
	 * will be notified about
	 */
	public static final String CLUSTER_NAMES = "cluster.names";

	/**
	 * This method is called to indicate that the cluster has changed in some way
	 * 
	 * @param cluster The cluster which has changed
	 * @param action The type of the change (member add, update or remove)
	 * @param id the Id of the node that has changed
	 * @param addedKeys the set of keys which are newly added
	 * @param removedKeys the set of keys which are removed
	 * @param updatedKeys the set of keys which have new values
	 */
	public void clusterEvent(ClusterInformation cluster, Action action, UUID id, Set<String> addedKeys, 
			Set<String> removedKeys, Set<String> updatedKeys);
	
}
