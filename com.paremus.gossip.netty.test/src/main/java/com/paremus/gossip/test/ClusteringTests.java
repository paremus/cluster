/*-
 * #%L
 * com.paremus.gossip.netty.test
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
package com.paremus.gossip.test;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.osgi.namespace.implementation.ImplementationNamespace.IMPLEMENTATION_NAMESPACE;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.osgi.annotation.bundle.Requirement;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;
import org.osgi.framework.wiring.FrameworkWiring;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.util.tracker.ServiceTracker;

import com.paremus.cluster.ClusterInformation;
import com.paremus.cluster.listener.Action;
import com.paremus.cluster.listener.ClusterListener;

@RunWith(JUnit4.class)
@Requirement(namespace=IMPLEMENTATION_NAMESPACE, name="com.paremus.cluster", version="1.0", effective="active")
public class ClusteringTests {

	private static final String FOO = "FOO";
    private static final byte[] BYTES = "FOO".getBytes();
    private static final String BAR = "BAR";
    private static final byte[] BYTES_2 = "BAR".getBytes();
    private static final String BAZ = "BAZ";
    private static final byte[] BYTES_3 = "BAZ".getBytes();
    private static final String FIZZ = "FIZZ";
    private static final byte[] BYTES_4 = "FIZZ".getBytes();
    private static final String BUZZ = "BUZZ";
    private static final byte[] BYTES_5 = "BUZZ".getBytes();
    private static final String FIZZBUZZ = "FIZZBUZZ";
    private static final byte[] BYTES_6 = "FIZZBUZZ".getBytes();

    private final AtomicInteger count = new AtomicInteger();

	private final BundleContext context = FrameworkUtil.getBundle(this.getClass()).getBundleContext();
	
	private final Semaphore sem = new Semaphore(0);

    private final ConcurrentMap<UUID, Map<String, String>> data = new ConcurrentHashMap<>();
	private List<Framework> childFrameworks;
	private int numberOfInsecureFrameworks = Math.min(Runtime.getRuntime().availableProcessors() * 6, 24);
	private int numberOfSecureFrameworks = Math.min(Runtime.getRuntime().availableProcessors() * 3, 12);
    
	private final List<ServiceRegistration<ClusterListener>> registered = new ArrayList<>();
	
	@Before
	public void setup() {
		// This is necessary due to a leak seen in Netty due to the finalization of
		// the buffer pool...
		System.setProperty("io.netty.allocator.type", "unpooled");
	}
	
	@After
	public void tearDown() throws Exception {
		
		registered.forEach(sr -> {
			try {
				sr.unregister();
			} catch (IllegalStateException ise) {
				// Just eat it
			}
		});
		registered.clear();
		
		if(childFrameworks != null) {
			childFrameworks.forEach((fw) -> { 
				try{
					fw.stop();
				} catch (BundleException be){}
			});
			childFrameworks.forEach((fw) -> { 
				try{
					fw.waitForStop(3000);
				} catch (Exception e){}
			});
		}
		
		ServiceReference<ConfigurationAdmin> ref = context.getServiceReference(ConfigurationAdmin.class);
		if(ref != null) {
			ConfigurationAdmin cm = context.getService(ref);
			if(cm != null) {
				Configuration[] configs = cm.listConfigurations(null);
				if(configs != null) {
					for(Configuration c : configs) {
						c.delete();
					}
				}
			}
		}
		Thread.sleep(5000);
	}
	
    private void listenerClusterEvent(ClusterInformation ci, Action action, UUID id, Set<String> addedKeys, 
			Set<String> removedKeys, Set<String> updatedKeys) {
    	
    	Map<String, String> map = data.computeIfAbsent(id, i -> new ConcurrentHashMap<>());
    	
    	Map<String, byte[]> attrs = ci.getMemberAttributes(id);

    	map.keySet().removeIf(removedKeys::contains);
    	
    	map.putAll(Stream.concat(addedKeys.stream(), updatedKeys.stream()).collect(Collectors.toMap(Function.identity(), s -> new String(attrs.get(s)))));

    	sem.release();
    }
    
    @Test
    public void testClusterPropagation() throws Exception {
    	System.out.println("Testing with " + numberOfInsecureFrameworks + " insecure cluster members");
    	doTestClusterPropagation(numberOfInsecureFrameworks, false);
    }
    
    @Test
    public void testClusterPropagationSecure() throws Exception {
    	System.out.println("Testing with " + numberOfSecureFrameworks + " secure cluster members");
    	
    	doTestClusterPropagation(numberOfSecureFrameworks, true);
    }
    
    private void doTestClusterPropagation(int size, boolean secure) throws Exception {
    	
    	Framework rootFw = context.getBundle(0).adapt(Framework.class);
    	configureFramework(rootFw, secure);
    	
    	Thread.sleep(1000);
    	
    	assertTrue(checkClusterInformation(rootFw, singletonMap(getUUID(rootFw), emptyMap())));
    	
    	updateClusterEntry(rootFw, FOO, BYTES);
    	
    	assertTrue(checkClusterInformation(rootFw, singletonMap(getUUID(rootFw), singletonMap(FOO, BYTES))));
    	
    	childFrameworks = createFrameworks(size);
        childFrameworks.stream().forEach(f -> configureFramework(f, secure));
        Thread.sleep(1000);
        
        List<Framework> cluster = Stream.concat(Stream.<Framework>builder().add(rootFw).build(), 
        		childFrameworks.stream()).collect(toList());
        
        Map<UUID, Map<String, byte[]>> expected = cluster.stream().map(this::getUUID)
        		.collect(toMap(identity(), (u) -> new HashMap<>()));
        expected.put(getUUID(rootFw), new HashMap<>(singletonMap(FOO, BYTES)));
        
        long start = System.currentTimeMillis();
        while(System.currentTimeMillis() - start < 20000) {
        	if(cluster.stream().allMatch(fw -> checkClusterInformation(fw, expected))) {
        		break;
        	}
        	Thread.sleep(100);
        }
        
        assertTrue(cluster.stream().allMatch(fw -> checkClusterInformation(fw, expected)));
        
        System.out.println("Established cluster after " + (System.currentTimeMillis() - start) + " milliseconds");
        
        for(int i =0; i < 4; i++) {
        	addRandomDataToCluster(cluster, expected);
        	start = System.currentTimeMillis();
        	while(System.currentTimeMillis() - start < 20000) {
        		if(cluster.stream().allMatch(fw -> checkClusterInformation(fw, expected))) {
        			break;
        		}
        		Thread.sleep(100);
        	}
        	
        	assertTrue(cluster.stream().allMatch(fw -> checkClusterInformation(fw, expected)));
        	
        	System.out.println("Established cluster after " + (System.currentTimeMillis() - start) + " milliseconds");
        }
        
    }

	private void addRandomDataToCluster(List<Framework> cluster,
			Map<UUID, Map<String, byte[]>> expected) {
		Random r = new Random();
        
        Framework f = cluster.get(r.nextInt(cluster.size()));
        updateClusterEntry(f, BAR, BYTES_2);
        expected.getOrDefault(getUUID(f), new HashMap<>()).put(BAR, BYTES_2);
        
        f = cluster.get(r.nextInt(cluster.size()));
        updateClusterEntry(f, BAZ, BYTES_3);
        expected.getOrDefault(getUUID(f), new HashMap<>()).put(BAZ, BYTES_3);

        f = cluster.get(r.nextInt(cluster.size()));
        updateClusterEntry(f, FIZZ, BYTES_4);
        expected.getOrDefault(getUUID(f), new HashMap<>()).put(FIZZ, BYTES_4);
        
        f = cluster.get(r.nextInt(cluster.size()));
        updateClusterEntry(f, BUZZ, BYTES_5);
        expected.getOrDefault(getUUID(f), new HashMap<>()).put(BUZZ, BYTES_5);
        
        f = cluster.get(r.nextInt(cluster.size()));
        updateClusterEntry(f, FIZZBUZZ, BYTES_6);
        expected.getOrDefault(getUUID(f), new HashMap<>()).put(FIZZBUZZ, BYTES_6);
	}
    
	@Test
	public void testListener() throws Exception {
		System.out.println("Testing with " + numberOfInsecureFrameworks + " insecure cluster members");
		doTestListener(numberOfInsecureFrameworks, false);
	}

	@Test
	public void testListenerSecure() throws Exception {
		
		System.out.println("Testing with " + numberOfSecureFrameworks + " secure cluster members");
		
		doTestListener(numberOfSecureFrameworks, true);
	}
	
    private void doTestListener(int size, boolean secure) throws Exception {
    	registered.add(context.registerService(ClusterListener.class, this::listenerClusterEvent, null));
    	
    	Framework rootFw = context.getBundle(0).adapt(Framework.class);
    	configureFramework(rootFw, secure);
    	
    	assertTrue(sem.tryAcquire(10000, TimeUnit.MILLISECONDS));
    	
    	assertEquals(emptyMap(), data.get(getUUID(rootFw)));
    	
    	updateClusterEntry(rootFw, FOO, BYTES);
    	
    	assertTrue(sem.tryAcquire(1000, TimeUnit.MILLISECONDS));
    
    	assertEquals(FOO, data.get(getUUID(rootFw)).get(FOO));
    	
    	childFrameworks = createFrameworks(size);
        childFrameworks.stream().forEach(f -> configureFramework(f, secure));
        
        List<Framework> cluster = Stream.concat(Stream.<Framework>builder().add(rootFw).build(), 
        		childFrameworks.stream()).collect(toList());
        
        Map<UUID, Map<String, byte[]>> expected = cluster.stream().map(this::getUUID)
        		.collect(toMap(identity(), (u) -> new HashMap<>()));
        expected.put(getUUID(rootFw), new HashMap<>(singletonMap(FOO, BYTES)));
        
        
        long start = System.currentTimeMillis();
        addRandomDataToCluster(cluster, expected);
        
        boolean allPresentAndCorrect = false;
        
        while(!allPresentAndCorrect && sem.tryAcquire(10000, TimeUnit.MILLISECONDS)) {
        	sem.drainPermits();
        	allPresentAndCorrect = expected.entrySet().stream().allMatch((e) -> {
        		Map<String, String> map = data.get(e.getKey());
        		if(map == null) {
        			return false;
        		} else {
        			return e.getValue().entrySet().stream().allMatch((e2) -> new String(e2.getValue()).equals(map.get(e2.getKey())));
        		}
        	});
        }
        
        assertTrue(allPresentAndCorrect);
        
        System.out.println("Consistent view after " + (System.currentTimeMillis() - start) + " milliseconds");
    }

    @Test
    public void testEsfSelection() throws Exception {
    	
    	System.out.println("Testing with " + numberOfSecureFrameworks + " secure cluster members");
    	
 
    	registered.add(context.registerService(ClusterListener.class, this::listenerClusterEvent, null));
    	
    	ServiceReference<ConfigurationAdmin> ref = context.getServiceReference(ConfigurationAdmin.class);
		assertNotNull(ref);
    	
    	ConfigurationAdmin cm = context.getService(ref);
    	assertNotNull(cm);
			
    	Configuration encodingConfigA = cm.createFactoryConfiguration("com.paremus.netty.tls", "?");
    	Configuration encodingConfigB = cm.createFactoryConfiguration("com.paremus.netty.tls", "?");
    	
    	encodingConfigA.update(getTlsConfig(false));
    	encodingConfigB.update(getTlsConfig(true));
    	
    	// Pick B over A even though it's a "lower" service
    	Configuration gossipConfig = cm.createFactoryConfiguration("com.paremus.gossip.netty", "?");
    	Dictionary<String, Object> gossip = getGossipConfig(count.getAndIncrement());
    	gossip.put("ssl.target", "(keystore.location=*)");
		gossipConfig.update(gossip);
    	
    	Framework rootFw = context.getBundle(0).adapt(Framework.class);
    	
    	assertTrue(sem.tryAcquire(10000, TimeUnit.MILLISECONDS));
    	
    	assertEquals(emptyMap(), data.get(getUUID(rootFw)));
    	
    	updateClusterEntry(rootFw, FOO, BYTES);
    	
    	assertTrue(sem.tryAcquire(1000, TimeUnit.MILLISECONDS));
    	
    	assertEquals(FOO, data.get(getUUID(rootFw)).get(FOO));
    	
    	childFrameworks = createFrameworks(numberOfSecureFrameworks);
    	childFrameworks.stream().forEach(f -> configureFramework(f, true));
    	
    	List<Framework> cluster = Stream.concat(Stream.<Framework>builder().add(rootFw).build(), 
    			childFrameworks.stream()).collect(toList());
    	
    	Map<UUID, Map<String, byte[]>> expected = cluster.stream().map(this::getUUID)
    			.collect(toMap(identity(), (u) -> new HashMap<>()));
    	expected.put(getUUID(rootFw), new HashMap<>(singletonMap(FOO, BYTES)));
    	
    	
    	long start = System.currentTimeMillis();
    	addRandomDataToCluster(cluster, expected);
    	
    	boolean allPresentAndCorrect = false;
    	
    	while(!allPresentAndCorrect && sem.tryAcquire(10000, TimeUnit.MILLISECONDS)) {
    		sem.drainPermits();
    		allPresentAndCorrect = expected.entrySet().stream().allMatch((e) -> {
    			Map<String, String> map = data.get(e.getKey());
    			if(map == null) {
    				return false;
    			} else {
    				return e.getValue().entrySet().stream().allMatch((e2) -> new String(e2.getValue()).equals(map.get(e2.getKey())));
    			}
    		});
    	}
    	
    	assertTrue(allPresentAndCorrect);
    	
    	System.out.println("Consistent view after " + (System.currentTimeMillis() - start) + " milliseconds");
    }
    
    @Test
    public void testLiveliness() throws Exception {
    	System.out.println("Testing with " + numberOfInsecureFrameworks + " insecure cluster members");
    	doTestLiveliness(numberOfInsecureFrameworks, false);
    }
    
    @Test
    public void testLivelinessSecure() throws Exception {
    	System.out.println("Testing with " + numberOfSecureFrameworks + " secure cluster members");
    	
    	doTestLiveliness(numberOfSecureFrameworks, true);
    }
    
    private void doTestLiveliness(int size, boolean secure) throws Exception {
    	
    	Framework rootFw = context.getBundle(0).adapt(Framework.class);
    	configureFramework(rootFw, secure);
    	
    	Semaphore sRemove = new Semaphore(0);
    	Semaphore sAdd = new Semaphore(0);
    	
    	ServiceRegistration<ClusterListener> reg = registerLivelinessListener(sRemove, sAdd);
		registered.add(reg);
    	
    	Thread.sleep(1000);
    	
    	updateClusterEntry(rootFw, FOO, BYTES);
    	
    	childFrameworks = createFrameworks(size);
        childFrameworks.stream().forEach(f -> configureFramework(f, secure));
        Thread.sleep(1000);
        
        List<Framework> cluster = Stream.concat(Stream.<Framework>builder().add(rootFw).build(), 
        		childFrameworks.stream()).collect(toList());
        
        Map<UUID, Map<String, byte[]>> expected = cluster.stream().map(this::getUUID)
        		.collect(toMap(identity(), (u) -> new HashMap<>()));
        expected.put(getUUID(rootFw), new HashMap<>(singletonMap(FOO, BYTES)));
        
        long start = System.currentTimeMillis();
        while(System.currentTimeMillis() - start < 20000) {
        	if(cluster.stream().allMatch(fw -> checkClusterInformation(fw, expected))) {
        		break;
        	}
        	Thread.sleep(100);
        }
        
        assertTrue(cluster.stream().allMatch(fw -> checkClusterInformation(fw, expected)));
        
        System.out.println("Established cluster after " + (System.currentTimeMillis() - start) + " milliseconds");
        
        boolean allAdded = sAdd.tryAcquire(size, 1, TimeUnit.SECONDS);

        if(!allAdded && sAdd.availablePermits() == 0) {
        	// This only seems to happen when running in the GitLab docker build...
        	System.out.println("CLUSTER LISTENER ERROR DETECTED!!!");
        	registered.remove(reg);
        	reg.unregister();
        	
        	// Re-registering seems to fix it...
        	registered.add(registerLivelinessListener(sRemove, sAdd));
        	
        	allAdded = sAdd.tryAcquire(size, 1, TimeUnit.SECONDS);
        }
        
        
		assertTrue(allAdded);
        assertTrue(sAdd.availablePermits() == 0);
        
        assertFalse(sRemove.tryAcquire(40, TimeUnit.SECONDS));
        
    }

	private ServiceRegistration<ClusterListener> registerLivelinessListener(Semaphore sRemove, Semaphore sAdd) {
		return context.registerService(ClusterListener.class, (f, a, id, s1, s2, s3) -> {
    		System.out.println("Primary framework received a Cluster Event " + a + " for node " + id);
    		if(a == Action.REMOVED) {
    			sRemove.release();
    			System.out.println("Lost contact with " + id);
    		} else if (a == Action.ADDED) {
    			System.out.println("Discovered node " + id);
    			sAdd.release();
    		}
    	}, null);
	}

	private void configureFramework(Framework f, boolean secure) {
    	try {
	    	@SuppressWarnings({ "unchecked", "rawtypes" })
			ServiceTracker tracker = new ServiceTracker(f.getBundleContext(), ConfigurationAdmin.class.getName(), null);
	    	tracker.open();
	    	Object cm = tracker.waitForService(2000);
	    	
	    	Method m = cm.getClass().getMethod("createFactoryConfiguration", String.class, String.class);
	    	
	    	Object gossipConfig = m.invoke(cm, "com.paremus.gossip.netty", "?");
	    	Object tlsConfig = m.invoke(cm, "com.paremus.netty.tls", "?");
	    	
	    	m = gossipConfig.getClass().getMethod("update", Dictionary.class);
	    	
	    	m.invoke(gossipConfig, getGossipConfig(count.getAndIncrement()));
	    	m.invoke(tlsConfig, getTlsConfig(secure));
	    	tracker.close();
    	} catch (Exception e) {
    		throw new RuntimeException(e);
    	}
    }

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private boolean checkClusterInformation(Framework f, Map<UUID, Map<String, byte[]>> expected) {
		AtomicBoolean answer = new AtomicBoolean(true);
		try {
			ServiceTracker tracker = new ServiceTracker(f.getBundleContext(), ClusterInformation.class.getName(), null);
			tracker.open();
			Object ci = tracker.waitForService(10000);
			Method m = ci.getClass().getMethod("getKnownMembers");
			
			Collection<UUID> ids = (Collection<UUID>)m.invoke(ci);
			
			if(expected.size() != ids.size()) {
				answer.set(false);
			}
			
        	List<UUID> unknown = expected.keySet().stream().collect(toList());
        	unknown.removeIf(ids::contains);
			
        	if(!unknown.isEmpty()) {
        		System.out.println(getUUID(f).toString() + " is still waiting for members: " + unknown);
        		answer.set(false);
        	}
        	
        	Method m2 = ci.getClass().getMethod("getMemberAttributes", UUID.class);
        	
        	ids.stream().forEach((id) ->  {
        		try {
        			Map<String, byte[]> attrs = (Map<String, byte[]>) m2.invoke(ci, id);
        			Map<String, byte[]> expAttrs = expected.get(id);
        			if(attrs == null) {
        				System.out.println(id.toString() + " is missing keys: " + expAttrs.keySet());
        				answer.set(false);
        				return;
        			}
        			if(!attrs.keySet().equals(expAttrs.keySet())) {
        				System.out.println("The view of " + id.toString() + " from " + getUUID(f) + 
        						" has the wrong keys, expected: " + expAttrs.keySet() + " but was " + attrs.keySet());
        				answer.set(false);
        				return;
        			}
        			expAttrs.entrySet().forEach((e) -> {
        				if(!Arrays.equals(e.getValue(), attrs.get(e.getKey()))) {
        					System.out.println("The view of " + id.toString() + " from " + getUUID(f) + 
        							" has the wrong value for key: " + e.getKey());
        					answer.set(false);
        				}
        			});
        		} catch (Exception e) {
        			throw new RuntimeException(e);
        		}
        	});
        	
			tracker.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return answer.get();
	}
    
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void updateClusterEntry(Framework f, String key, byte[] value) {
		try {
			ServiceTracker tracker = new ServiceTracker(f.getBundleContext(), ClusterInformation.class.getName(), null);
			tracker.open();
			Object ci = tracker.waitForService(4000);
			Method m = ci.getClass().getMethod("updateAttribute", String.class, byte[].class);
			m.invoke(ci, key, value);
			tracker.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private Dictionary<String, Object> getGossipConfig(int i) {
		Hashtable<String, Object> config = new Hashtable<String, Object>();
		config.put("bind.address", "127.0.0.1");
		config.put("udp.port", 17001 + 100 * i);
		config.put("tcp.port", 17002 + 100 * i);
		config.put("initial.peers", new String[] {"127.0.0.1:17001", "127.0.0.1:17101", "127.0.0.1:17201"});
		config.put("cluster.name", "clusterOne");
		
		return config;
	}
	
	private Dictionary<String, Object> getTlsConfig(boolean secure) {
		Hashtable<String, Object> config = new Hashtable<String, Object>();
		
		if(secure) {
			String testResources = context.getProperty("test.resources");
			
			config.put("keystore.location", testResources + "etc/testSign.keystore");
			config.put("keystore.type", "jks");
			config.put(".keystore.password", "testingSign");
			config.put(".keystore.key.password", "signing");

			config.put("truststore.location", testResources + "etc/test.truststore");
			config.put("truststore.type", "jks");
			config.put(".truststore.password", "paremus");
		} else {
			config.put("insecure", true);
		}
		
		return config;
	}

	private List<Framework> createFrameworks(int size) throws BundleException {
		FrameworkFactory ff = ServiceLoader.load(FrameworkFactory.class, 
    			context.getBundle(0).adapt(ClassLoader.class)).iterator().next();
    	
    	List<String> locations = new ArrayList<>();
    	
    	for(Bundle b : context.getBundles()) {
    		if(b.getSymbolicName().equals("org.apache.felix.configadmin") ||
    				b.getSymbolicName().equals("org.apache.felix.scr") ||
    				b.getSymbolicName().equals("com.paremus.cluster.api") ||
    				b.getSymbolicName().equals("com.paremus.gossip.netty") ||
    				b.getSymbolicName().equals("com.paremus.netty.tls") ||
    				b.getSymbolicName().equals("com.paremus.license") ||
    				b.getSymbolicName().startsWith("io.netty") ||
    				b.getSymbolicName().startsWith("bc") ||
    				b.getSymbolicName().startsWith("org.osgi.util") ||
    				b.getSymbolicName().startsWith("slf4j") ||
    				b.getSymbolicName().startsWith("ch.qos.logback")) {
    			locations.add(b.getLocation());
    		}
    	}
    	
    	List<Framework> clusterOne = new ArrayList<Framework>();
        for(int i = 1; i < size; i++) {
        	Map<String, String> fwConfig = new HashMap<>();
        	fwConfig.put(Constants.FRAMEWORK_STORAGE, new File(context.getDataFile(""), "ClusterOne" + i).getAbsolutePath());
        	fwConfig.put(Constants.FRAMEWORK_STORAGE_CLEAN, Constants.FRAMEWORK_STORAGE_CLEAN_ONFIRSTINIT);
        	Framework f = ff.newFramework(fwConfig);
        	clusterOne.add(f);
        	f.init();
        	for(String s : locations) {
        		f.getBundleContext().installBundle(s);
        	}
        	f.start();
        	f.adapt(FrameworkWiring.class).resolveBundles(Collections.emptySet());
        	for(Bundle b : f.getBundleContext().getBundles()) {
        		if(b.getHeaders().get(Constants.FRAGMENT_HOST) == null) {
        			b.start();
        		}
        	}
        }
		return clusterOne;
	}

	private UUID getUUID(Framework f) {
		return UUID.fromString(f.getBundleContext().getProperty(Constants.FRAMEWORK_UUID));
	}
}
