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
package com.paremus.gossip.netty;

import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@ObjectClassDefinition(factoryPid="com.paremus.gossip")
public @interface Config {

	int udp_port() default 9033;
	int tcp_port() default 9034;
	
	String bind_address() default "0.0.0.0";

	@AttributeDefinition(min="50")
	int gossip_interval() default 300;
	@AttributeDefinition(min="1", max="6")
	int gossip_fanout() default 2;
	@AttributeDefinition(min="1", max="5")
	int gossip_hops() default 3;
	@AttributeDefinition(min="1")
	int gossip_broadcast_rounds() default 20;

	@AttributeDefinition(min="0")
	long sync_interval() default 20000;
	@AttributeDefinition(min="0")
	long sync_retry() default 1000;
	

	String[] initial_peers();
	String cluster_name();
	
	String tls_target() default "";
	
	boolean infra() default false;
	
	@AttributeDefinition(min="0")
	int silent_node_probe_timeout() default 12000;
	@AttributeDefinition(min="0")
	int silent_node_eviction_timeout() default 15000;
}
