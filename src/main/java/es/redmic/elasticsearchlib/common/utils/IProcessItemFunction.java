package es.redmic.elasticsearchlib.common.utils;

import java.util.List;

import org.elasticsearch.search.SearchHit;

public interface IProcessItemFunction<T> {

	void process(SearchHit hit);

	List<?> getResults();

}
