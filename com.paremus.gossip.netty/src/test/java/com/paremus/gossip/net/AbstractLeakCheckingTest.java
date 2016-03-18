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
package com.paremus.gossip.net;

import static io.netty.util.ResourceLeakDetector.Level.PARANOID;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.LoggerFactory;

import io.netty.util.HashedWheelTimer;
import io.netty.util.ResourceLeakDetector;

public abstract class AbstractLeakCheckingTest {

	@Rule
	public TestName name = new TestName();

	@Before
	public final void setupLeakChecking() throws IOException {
		
		System.setProperty("io.netty.leakDetection.maxRecords", "100");
        System.setProperty("io.netty.customResourceLeakDetector", TestResourceLeakDetector.class.getName());
        TestResourceLeakDetector.addResourceTypeToIgnore(HashedWheelTimer.class);
	    
		ResourceLeakDetector.setLevel(PARANOID);
		LoggerFactory.getLogger(getClass()).info("Beginning test {}", name.getMethodName());
	}
	
    @After
    public final void leakCheck() throws IOException {
    	TestResourceLeakDetector.assertNoLeaks();
    	TestResourceLeakDetector.clearIgnoredResourceTypes();
    }
}
