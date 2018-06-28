package es.redmic.elasticsearchlib.common.query;

import java.util.List;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import es.redmic.models.es.common.query.dto.RegexpDTO;
import es.redmic.models.es.common.query.dto.SimpleQueryDTO;
import es.redmic.models.es.common.query.dto.TextQueryDTO;

public abstract class SimpleQueryUtils {

	// @formatter:off

	public static final String INTRACK_PATH = "properties.inTrack",
			COLLECT_PATH = "properties.collect",
			SITE_PATH = "properties.site",
			SAMPLINGPLACE_PATH = "properties.samplingPlace",
			MEASUREMENT_PATH = "properties.measurements",

			DATA_DEFINITION_PROPERTY = "dataDefinition",

			ID_PROPERTY = "id",
			QFLAG_PROPERTY = "qFlag",
			VFLAG_PROPERTY = "vFlag",
			START_DATE_PROPERTY = "startDate",
			END_DATE_PROPERTY = "endDate",
			DATE_PROPERTY = "date",
			RADIUS_PROPERTY = "radius";

	// @formatter:on

	public static BoolQueryBuilder getQuery(SimpleQueryDTO queryDTO, QueryBuilder internalQuery,
			QueryBuilder partialQuery) {
		return getBaseQuery(queryDTO, internalQuery, partialQuery);
	}

	protected static BoolQueryBuilder getBaseQuery(SimpleQueryDTO queryDTO, QueryBuilder internalQuery,
			QueryBuilder partialQuery) {

		BoolQueryBuilder query = QueryBuilders.boolQuery();

		addMustTermIfExist(query, getTextQuery(queryDTO.getText()));
		addMustTermIfExist(query, getRegexpQuery(queryDTO.getRegexp()));
		addMustTermIfExist(query, partialQuery);
		addMustTermIfExist(query, internalQuery);

		return query.hasClauses() ? query : null;
	}

	protected static QueryBuilder getTextQuery(TextQueryDTO queryText) {

		if (queryText == null || queryText.getText() == null || queryText.getSearchFields() == null)
			return null;

		return QueryBuilders.multiMatchQuery(queryText.getText(), queryText.getSearchFields());
	}

	protected static BoolQueryBuilder getRegexpQuery(List<RegexpDTO> regexp) {

		if (regexp == null || regexp.size() < 1)
			return null;

		BoolQueryBuilder regexpQuery = QueryBuilders.boolQuery();

		int size = regexp.size();
		for (int i = 0; i < size; i++)
			regexpQuery.must(QueryBuilders.regexpQuery(regexp.get(i).getField(), regexp.get(i).getExp()));

		return regexpQuery;
	}

	protected static void addMustTermIfExist(BoolQueryBuilder baseQuery, QueryBuilder term) {

		if (term != null)
			baseQuery.must(term);
	}

	protected static BoolQueryBuilder getOrInitializeBaseQuery(BoolQueryBuilder baseQuery) {

		if (baseQuery == null)
			baseQuery = QueryBuilders.boolQuery();
		return baseQuery;
	}

	protected static BoolQueryBuilder getResultQuery(BoolQueryBuilder query) {

		if (query.hasClauses())
			return query;
		return null;
	}
}