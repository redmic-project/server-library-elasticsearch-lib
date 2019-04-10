package es.redmic.elasticsearchlib.timeseries.repository;

import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.format.DateTimeFormat;

import es.redmic.elasticsearchlib.data.repository.RDataESRepository;
import es.redmic.models.es.common.model.BaseTimeDataAbstractES;
import es.redmic.models.es.common.query.dto.DataQueryDTO;

public abstract class RTimeSeriesESRepository<TModel extends BaseTimeDataAbstractES, TQueryDTO extends DataQueryDTO>
		extends RDataESRepository<TModel, TQueryDTO> implements IBaseTimeSeriesESRepository {

	public RTimeSeriesESRepository() {
		super(IBaseTimeSeriesESRepository.INDEX, IBaseTimeSeriesESRepository.TYPE, true);
	}

	@Override
	protected String getIndex(final TModel modelToIndex) {
		return getIndex()[0] + "-" + modelToIndex.getDate().toString(DateTimeFormat.forPattern("yyyy-MM-dd"));
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryBuilder getTermQuery(Map<String, Object> terms, BoolQueryBuilder query) {

		if (terms.containsKey("dataDefinition")) {
			List<Integer> ids = (List<Integer>) terms.get("dataDefinition");
			query.must(QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery("dataDefinition", ids)));
		}

		if (terms.containsKey("dates")) {
			List<String> dates = (List<String>) terms.get("dates");
			query.must(QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery("date", dates)));
		}
		return super.getTermQuery(terms, query);
	}
}
