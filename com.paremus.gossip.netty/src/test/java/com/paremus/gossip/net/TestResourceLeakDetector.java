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

import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import io.netty.util.ResourceLeakDetector;

public class TestResourceLeakDetector<T> extends ResourceLeakDetector<T> {

    private static final List<String> leaks = new Vector<>();
    
    private static final Set<String> resourceTypesToIgnore = new HashSet<>();
    
    public TestResourceLeakDetector(Class<T> resourceType, int samplingInterval) {
        super(resourceType, samplingInterval);
    }
    public TestResourceLeakDetector(Class<T> resourceType, int samplingInterval, long l) {
        super(resourceType, samplingInterval);
    }

    @Override
    protected void reportTracedLeak(String resourceType, String records) {
        
    	if(!resourceTypesToIgnore.contains(resourceType)) {
    		leaks.add("\nRecord:\n" + resourceType + "\n" + records + "\n");
    	}
        super.reportTracedLeak(resourceType, records);
    }

    @Override
    protected void reportUntracedLeak(String resourceType) {
    	if(!resourceTypesToIgnore.contains(resourceType)) {
    		leaks.add("\nRecord:\n" + resourceType + "\n");
    	}
        super.reportUntracedLeak(resourceType);
    }

    public static void assertNoLeaks() {
        System.gc();
        assertTrue(leaks.toString(), leaks.isEmpty());
        leaks.clear();
    }

    public static void addResourceTypeToIgnore(Class<?> clazz) {
    	resourceTypesToIgnore.add(clazz.getSimpleName());
    }
    
    public static void clearIgnoredResourceTypes() {
    	resourceTypesToIgnore.clear();
    }
}
