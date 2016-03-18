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

/**
 * The action associated with a cluster event
 */
public enum Action {
	/**
	 * The member is being added
	 */
	ADDED, 
	/**
	 * The properties stored by the member have
	 * been updated
	 */
	UPDATED, 
	/**
	 * The member is being removed
	 */
	REMOVED;
}
