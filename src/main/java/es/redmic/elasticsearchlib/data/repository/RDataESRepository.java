package es.redmic.elasticsearchlib.data.repository;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.fasterxml.jackson.databind.JavaType;

import es.redmic.elasticsearchlib.common.repository.RBaseESRepository;
import es.redmic.models.es.common.model.BaseES;
import es.redmic.models.es.common.query.dto.MgetDTO;
import es.redmic.models.es.common.query.dto.SimpleQueryDTO;
import es.redmic.models.es.data.common.model.DataHitWrapper;
import es.redmic.models.es.data.common.model.DataHitsWrapper;
import es.redmic.models.es.data.common.model.DataSearchWrapper;

public abstract class RDataESRepository<TModel extends BaseES<?>, TQueryDTO extends SimpleQueryDTO>
		extends RBaseESRepository<TModel, TQueryDTO> {

	public RDataESRepository() {
	}

	public RDataESRepository(String[] index, String type) {
		super(index, type);
	}

	public DataHitWrapper<?> findById(String id) {

		return getResponseToWrapper(getRequest(id), getSourceType(DataHitWrapper.class));
	}

	public DataSearchWrapper<?> searchByIds(String[] ids) {
		return findBy(QueryBuilders.idsQuery().addIds(ids));
	}

	protected DataSearchWrapper<?> findBy(QueryBuilder queryBuilder) {

		return findBy(queryBuilder, SortBuilders.fieldSort("id").order(SortOrder.ASC), null);
	}

	protected DataSearchWrapper<?> findBy(QueryBuilder queryBuilder, SortBuilder<?> sort) {

		return searchResponseToWrapper(searchRequest(queryBuilder, sort, null), getSourceType(DataSearchWrapper.class));
	}

	protected DataSearchWrapper<?> findBy(QueryBuilder queryBuilder, List<String> returnFields) {

		return findBy(queryBuilder, SortBuilders.fieldSort("id").order(SortOrder.ASC), returnFields);
	}

	protected DataSearchWrapper<?> findBy(QueryBuilder queryBuilder, SortBuilder<?> sort, List<String> returnFields) {

		return searchResponseToWrapper(searchRequest(queryBuilder, sort, returnFields),
				getSourceType(DataSearchWrapper.class));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public DataHitsWrapper<?> mget(MgetDTO dto) {

		MultiGetResponse result = multigetRequest(dto);

		List<DataHitWrapper<?>> hits = mgetResponseToWrapper(result, getSourceType(DataHitWrapper.class));
		DataHitsWrapper data = new DataHitsWrapper(hits);
		data.setTotal(hits.size());
		return data;
	}

	public DataSearchWrapper<?> find(TQueryDTO queryDTO) {

		return searchResponseToWrapper(searchRequest(queryDTO), getSourceType(DataSearchWrapper.class));
	}

	public List<DataSearchWrapper<?>> multiFind(List<SearchSourceBuilder> searchs) {

		List<DataSearchWrapper<?>> results = new ArrayList<DataSearchWrapper<?>>();

		MultiSearchResponse resultRequest = getMultiFindResponses(searchs);

		for (MultiSearchResponse.Item item : resultRequest.getResponses()) {
			SearchResponse response = item.getResponse();
			results.add(searchResponseToWrapper(response, getSourceType(DataSearchWrapper.class)));
		}
		return results;
	}

	@Override
	protected List<?> scrollQueryReturnItems(QueryBuilder builder) {

		return scrollQueryReturnItems(builder, new DataItemsProcessingFunction<TModel>(typeOfTModel, objectMapper));
	}

	@Override
	protected JavaType getSourceType(Class<?> wrapperClass) {
		return objectMapper.getTypeFactory().constructParametricType(wrapperClass, typeOfTModel);
	}
}