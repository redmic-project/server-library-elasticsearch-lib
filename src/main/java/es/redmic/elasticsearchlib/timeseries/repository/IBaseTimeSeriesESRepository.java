package es.redmic.elasticsearchlib.timeseries.repository;

import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

public interface IBaseTimeSeriesESRepository {

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
