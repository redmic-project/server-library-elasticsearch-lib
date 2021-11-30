package es.redmic.elasticsearchlib.data.repository;

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

	protected RDataESRepository() {
	}

	protected RDataESRepository(String[] index, String type) {
		super(index, type);
	}

	protected RDataESRepository(String[] index, String type, Boolean rollOverIndex) {
		super(index, type, rollOverIndex);
	}

	public DataHitWrapper<?> findById(String id) {

		return getResponseToWrapper(getRequest(id), getSourceType(DataHitWrapper.class));
	}

	public DataHitWrapper<?> findById(String id, String parentId) {

		return getResponseToWrapper(getRequest(id, parentId), getSourceType(DataHitWrapper.class));
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
