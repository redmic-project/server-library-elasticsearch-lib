package es.redmic.elasticsearchlib.series.repository;

/*-
 * #%L
 * elasticsearch-lib
 * %%
 * Copyright (C) 2019 REDMIC Project / Server
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

public interface IBaseSeriesESRepository {

	static String[] INDEX = { "timeseries" };
	static String TYPE = "_doc";

	String defaultField = "value";

	String dateTimeField = "date";

	int minDocCount = 0;

	default DateHistogramAggregationBuilder getDateHistogramAggregation(DateHistogramInterval dateHistogramInterval) {

		return AggregationBuilders.dateHistogram("dateHistogram").field(dateTimeField)
				.dateHistogramInterval(dateHistogramInterval).minDocCount(minDocCount);
	}
}
