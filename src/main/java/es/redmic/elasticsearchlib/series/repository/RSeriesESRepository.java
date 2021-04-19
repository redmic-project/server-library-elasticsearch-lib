package es.redmic.elasticsearchlib.series.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JavaType;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import es.redmic.elasticsearchlib.common.query.SeriesQueryUtils;

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

import es.redmic.elasticsearchlib.common.repository.RBaseESRepository;
import es.redmic.exception.data.ItemNotFoundException;
import es.redmic.models.es.common.model.BaseAbstractStringES;
import es.redmic.models.es.common.query.dto.DataQueryDTO;
import es.redmic.models.es.common.query.dto.MgetDTO;
import es.redmic.models.es.series.common.model.SeriesHitWrapper;
import es.redmic.models.es.series.common.model.SeriesHitsWrapper;
import es.redmic.models.es.series.common.model.SeriesSearchWrapper;

public abstract class RSeriesESRepository<TModel extends BaseAbstractStringES, TQueryDTO extends DataQueryDTO>
		extends RBaseESRepository<TModel, TQueryDTO> implements IRSeriesESRepository<TModel> {

	private static final String DATADEFINITION_PROPERTY = "dataDefinition";

	protected RSeriesESRepository() {
		super(IBaseSeriesESRepository.INDEX, IBaseSeriesESRepository.TYPE, true);
	}

	@Override
	public SeriesHitWrapper<TModel> findById(String id) {

		BoolQueryBuilder query = SeriesQueryUtils.getItemsQuery(id);

		SeriesSearchWrapper<TModel> result = findBy(query);

		if (result.getHits() == null || result.getHits().getHits() == null || result.getHits().getHits().size() != 1)
			throw new ItemNotFoundException("id", id);

		return result.getHits().getHits().get(0);
	}

	@Override
	public SeriesHitsWrapper<TModel> mget(MgetDTO dto) {

		List<String> ids = dto.getIds();

		BoolQueryBuilder query = SeriesQueryUtils.getItemsQuery(ids);

		SeriesSearchWrapper<TModel> result = findBy(query, dto.getFields());

		if (result.getHits() == null || result.getHits().getHits() == null)
			throw new ItemNotFoundException("ids", dto.getIds().toString());

		if (result.getHits().getHits().size() != ids.size()) {

			for (SeriesHitWrapper<?> hit : result.getHits().getHits()) {
				ids.remove(hit.get_id());
			}

			throw new ItemNotFoundException("ids", ids.toString());
		}

		return result.getHits();
	}

	@Override
	public SeriesSearchWrapper<TModel> searchByIds(String[] ids) {

		return findBy(QueryBuilders.idsQuery().addIds(ids));
	}

	protected SeriesSearchWrapper<TModel> findBy(QueryBuilder queryBuilder) {

		return findBy(queryBuilder, null);
	}

	protected SeriesSearchWrapper<TModel> findBy(QueryBuilder queryBuilder, List<String> returnFields) {

		return searchResponseToWrapper(searchRequest(queryBuilder, returnFields),
				getSourceType(SeriesSearchWrapper.class));
	}

	@SuppressWarnings("unchecked")
	@Override
	public SeriesSearchWrapper<TModel> find(DataQueryDTO queryDTO) {

		QueryBuilder serviceQuery = SeriesQueryUtils.getQuery(queryDTO);

		SearchResponse result = searchRequest((TQueryDTO) queryDTO, serviceQuery);

		if (result.getFailedShards() > 0)
			return null;

		return searchResponseToWrapper(result, getSourceType(SeriesSearchWrapper.class));
	}

	@Override
	public List<SeriesSearchWrapper<TModel>> multiFind(List<SearchSourceBuilder> searchs) {

		List<SeriesSearchWrapper<TModel>> results = new ArrayList<>();

		MultiSearchResponse resultRequest = super.getMultiFindResponses(searchs);

		for (MultiSearchResponse.Item item : resultRequest.getResponses()) {
			SearchResponse response = item.getResponse();
			results.add(searchResponseToWrapper(response, getSourceType(SeriesSearchWrapper.class)));
		}
		return results;
	}

	/**
	 * Función que nos devuelve el size de la query El size del exterior tiene
	 * preferencia, en caso de no exista, se devuelve todo lo almacenado.
	 *
	 * @param size.
	 *            Size enviado desde queryDto
	 * @param queryDTO.
	 *            queryDto para obtener parámetros de query enviados por el cliente
	 *
	 * @return numero de elementos que devolverá la query
	 */
	@Override
	protected Integer getSize(TQueryDTO queryDTO, BoolQueryBuilder query) {

		if ((queryDTO.getAggs() != null && !queryDTO.getAggs().isEmpty()) || (queryDTO.getInterval() != null))
			return 0;

		return super.getSize(queryDTO, query);
	}

	/**
	 * Función que nos devuelve una lista de ordenaciones específica para
	 * timeseries. Por defecto, ordena por id.
	 *
	 * @return lista de ordenaciones de elasticsearch
	 */
	@Override
	protected List<SortBuilder<?>> getSort() {

		List<SortBuilder<?>> sorts = new ArrayList<>();
		sorts.add(SortBuilders.fieldSort(dateTimeField).order(SortOrder.ASC));
		return sorts;
	}

	/**
	 * Función que sobrescribe a getTermQuery de RElasticSearchRepository para
	 * añadir implementación específica para crear una query a apartir de una serie
	 * de términos obtenidos por el controlador.
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected QueryBuilder getTermQuery(Map<String, Object> terms, BoolQueryBuilder query) {

		if (terms.containsKey(DATADEFINITION_PROPERTY)) {
			List<Integer> ids = (List<Integer>) (List<?>) terms.get(DATADEFINITION_PROPERTY);
			query.must(QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery(DATADEFINITION_PROPERTY, ids)));
		}
		return super.getTermQuery(terms, query);
	}

	@Override
	protected List<?> scrollQueryReturnItems(QueryBuilder builder) {

		return scrollQueryReturnItems(builder, new SeriesItemsProcessingFunction<TModel>(typeOfTModel, objectMapper));
	}

	@Override
	protected JavaType getSourceType(Class<?> wrapperClass) {
		return objectMapper.getTypeFactory().constructParametricType(wrapperClass, typeOfTModel);
	}

	public DataQueryDTO createSimpleQueryDTOFromQueryParams(Integer from, Integer size) {

		DataQueryDTO queryDTO = new DataQueryDTO();

		if (from != null)
			queryDTO.setFrom(from);
		if (size != null)
			queryDTO.setSize(size);

		return queryDTO;
	}
}
