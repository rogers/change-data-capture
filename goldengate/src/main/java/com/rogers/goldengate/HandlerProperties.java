/*
 *
 * Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.
 *
 */
package com.rogers.goldengate;

import java.util.List;

public class HandlerProperties {

	public Boolean includeOpTimestamp;

	public Long totalInserts = 0L;
	public Long totalUpdates = 0L;
	public Long totalDeletes = 0L;
	public Long totalTxns = 0L;
	public Long totalOperations = 0L;

}
