package org.shirdrn.storm.analytics.mydis.common;

import java.util.Collection;

public interface UpdatePolicy<T> {

	void computeBatch(String sql, final Collection<T> values) throws Exception;
}
