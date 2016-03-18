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

import com.paremus.gossip.cluster.impl.MemberInfo;
import com.paremus.gossip.v1.messages.Snapshot;

import io.netty.util.concurrent.Future;

public interface InternalClusterListener {
	void localUpdate(Snapshot s);
	Future<?> destroy();
	void darkNodes(Collection<MemberInfo> darkNodes);
}
