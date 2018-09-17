package es.redmic.elasticsearchlib.common.repository;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import es.redmic.elasticsearchlib.common.query.SimpleQueryUtils;
import es.redmic.elasticsearchlib.common.utils.ElasticSearchUtils;
import es.redmic.elasticsearchlib.common.utils.IProcessItemFunction;
import es.redmic.elasticsearchlib.config.EsClientProvider;
import es.redmic.exception.data.ItemNotFoundException;
import es.redmic.exception.databinding.RequestNotValidException;
import es.redmic.exception.elasticsearch.ESNotExistsIndexException;
import es.redmic.exception.elasticsearch.ESNotExistsTypeException;
import es.redmic.models.es.common.model.BaseES;
import es.redmic.models.es.common.query.dto.AggsPropertiesDTO;
import es.redmic.models.es.common.query.dto.MgetDTO;
import es.redmic.models.es.common.query.dto.SimpleQueryDTO;
import es.redmic.models.es.common.query.dto.SortDTO;
import es.redmic.models.es.common.query.dto.SuggestQueryDTO;

public abstract class RBaseESRepository<TModel extends BaseES<?>, TQueryDTO extends SimpleQueryDTO> {

	protected final static Logger LOGGER = LoggerFactory.getLogger(RBaseESRepository.class);

	protected static String SCRIPT_ENGINE = "groovy";

	@Value("${redmic.elasticsearch.check.mappings}")
	private boolean checkMappings;

	@Autowired
	protected EsClientProvider ESProvider;

	private QueryBuilder INTERNAL_QUERY = null;

	private String[] INDEX;
	private String[] TYPE;

	protected Integer SUGGESTSIZE = 10;
	protected String PRETAGS = "<b>";
	protected String POSTTAGS = "</b>";

	protected Integer MAX_SIZE = 100000;

	@Autowired
	protected ObjectMapper objectMapper;

	protected Class<TModel> typeOfTModel;

	public RBaseESRepository() {
	}

	@SuppressWarnings("unchecked")
	public RBaseESRepository(String[] index, String[] type) {
		this.INDEX = index;
		this.TYPE = type;

		if (getClass().getGenericSuperclass() instanceof ParameterizedType) {
			this.typeOfTModel = (Class<TModel>) ((ParameterizedType) getClass().getGenericSuperclass())
					.getActualTypeArguments()[0];
		}
	}

	@PostConstruct
	private void checkIndicesAndTypes() {
		if (checkMappings && (INDEX != null && TYPE != null)) {
			checkExistsIndices();
			checkExistsTypes();
		}
	}

	/**
	 * Chequea que los índices existen
	 */
	private void checkExistsIndices() {

		for (int i = 0; i < INDEX.length; i++) {
			String index = INDEX[i];

			Boolean exists = ESProvider.getClient().admin().indices().exists(new IndicesExistsRequest(index))
					.actionGet().isExists();

			if (!exists) {
				throw new ESNotExistsIndexException(index);
			}
		}
	}

	/**
	 * Chequea que los tipos existen
	 */
	private void checkExistsTypes() {

		ClusterStateResponse resp = ESProvider.getClient().admin().cluster().prepareState().execute().actionGet();
		ImmutableOpenMap<String, IndexMetaData> indices = resp.getState().metaData().getIndices();

		for (int i = 0; i < INDEX.length; i++) {
			IndexMetaData index = indices.get(INDEX[i]);

			if (index == null) {
				AliasOrIndex aliases = resp.getState().getMetaData().getAliasAndIndexLookup().get(INDEX[i]);

				if (aliases == null || aliases.getIndices() == null || aliases.getIndices().size() == 0) {
					throw new ESNotExistsIndexException(INDEX[i]);
				}
				index = aliases.getIndices().get(0);
			}

			ImmutableOpenMap<String, MappingMetaData> mappings = index.getMappings();
			for (int j = 0; j < TYPE.length; j++) {
				String type = TYPE[j];
				if (!mappings.containsKey(type)) {
					throw new ESNotExistsTypeException(index.getIndex().getName(), type);
				}
			}
		}
	}

	protected abstract JavaType getSourceType(Class<?> wrapperClass);

	protected GetResponse getRequest(String id) {
		return getRequest(id, null, null);
	}

	protected GetResponse getRequest(String id, String parentId) {
		return getRequest(id, parentId, null);
	}

	protected GetResponse getRequest(String id, String parentId, String grandparentId) {

		LOGGER.debug("FindById en " + getIndex() + " " + getType() + " con id " + id);

		GetResponse response = null;

		for (int i = 0; i < getIndex().length; i++) {
			for (int j = 0; j < getType().length; j++) {
				GetRequestBuilder prepareGet = ESProvider.getClient().prepareGet(getIndex()[i], getType()[j],
						id.toString());

				if (parentId != null) {
					prepareGet.setParent(parentId);
				}
				if (grandparentId != null) {
					prepareGet.setRouting(grandparentId);
				}
				response = prepareGet.execute().actionGet();

				if (response != null && response.isExists()) {
					return response;
				}
			}
		}

		throw new ItemNotFoundException("id", id + " en el servicio " + getType()[0]);
	}

	public List<String> suggest(TQueryDTO queryDTO) {

		return suggest(queryDTO, null);
	}

	public List<String> suggest(TQueryDTO queryDTO, QueryBuilder partialQuery) {

		SuggestQueryDTO suggestDTO = queryDTO.getSuggest();

		if (suggestDTO == null) {
			throw new RequestNotValidException("suggest", null);
		}
		String[] fields = suggestDTO.getSearchFields();
		Integer size = suggestDTO.getSize();
		String text = suggestDTO.getText();

		QueryBuilder termQuery = getTermQuery(queryDTO.getTerms());

		if (termQuery != null && partialQuery != null) {
			partialQuery = QueryBuilders.boolQuery().must(partialQuery).must(termQuery);
		} else if (termQuery != null) {
			partialQuery = QueryBuilders.boolQuery().must(termQuery);
		}

		LOGGER.debug("Suggest en " + getIndex() + " " + getType() + " con fields " + fields + " y texto " + text
				+ " y query interna " + partialQuery);

		SearchRequestBuilder requestBuilder = ESProvider.getClient().prepareSearch(getIndex()).setTypes(getType());

		String[] suggestFields = ElasticSearchUtils.getSuggestFields(fields);

		BoolQueryBuilder queryBuilder = getQuery(queryDTO, getInternalQuery(), partialQuery);

		QueryBuilder query = QueryBuilders.multiMatchQuery(text, suggestFields)
				.type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX);

		if (queryBuilder != null) {
			query = QueryBuilders.boolQuery().must(query).must(queryBuilder);
		}

		requestBuilder.setQuery(query).setSize((size == null) ? SUGGESTSIZE : size)
				.highlighter(getHighlightBuilder(queryDTO.getSuggest().getSearchFields()));

		return ElasticSearchUtils.createHighlightResponse(requestBuilder.execute().actionGet());
	}

	protected MultiGetResponse multigetRequest(MgetDTO dto) {

		return multigetRequest(dto, null);
	}

	protected MultiGetResponse multigetRequest(MgetDTO dto, String parentId) {

		return multigetRequest(dto, parentId, null);
	}

	protected MultiGetResponse multigetRequest(MgetDTO dto, String parentId, String grandParentId) {

		LOGGER.debug("Mget en " + getIndex() + " " + getType() + " con fields " + dto.getFields() + " e ids "
				+ dto.getIds());

		MultiGetRequestBuilder builder = ESProvider.getClient().prepareMultiGet();

		FetchSourceContext fetchSourceContext;

		if (dto.getFields() == null || dto.getFields().size() == 0) {
			fetchSourceContext = new FetchSourceContext(true);
		} else {
			String[] fieldsArray = dto.getFields().toArray(new String[dto.getFields().size()]);
			fetchSourceContext = new FetchSourceContext(true, fieldsArray, new String[0]);
		}

		int sizeIds = dto.getIds().size();
		for (int i = 0; i < getIndex().length; i++) {
			for (int j = 0; j < getType().length; j++) {
				for (int k = 0; k < sizeIds; k++) {

					Item item = new Item(getIndex()[i], getType()[j], dto.getIds().get(k));

					if (parentId != null) {
						item.parent(parentId);
					}
					if (grandParentId != null) {
						item.routing(grandParentId);
					}
					item.fetchSourceContext(fetchSourceContext);
					builder.add(item);
				}
			}
		}
		return builder.execute().actionGet();
	}

	protected SearchResponse searchRequest(QueryBuilder query) {

		return searchRequest(query, SortBuilders.fieldSort("id").order(SortOrder.ASC), null);
	}

	protected SearchResponse searchRequest(QueryBuilder query, SortBuilder<?> sort) {

		return searchRequest(query, sort, null);
	}

	protected SearchResponse searchRequest(QueryBuilder query, List<String> returnFields) {

		return searchRequest(query, SortBuilders.fieldSort("id").order(SortOrder.ASC), returnFields);
	}

	protected SearchResponse searchRequest(QueryBuilder query, SortBuilder<?> sort, List<String> returnFields) {

		LOGGER.debug("FindBy query en " + getIndex() + " " + getType() + " con query " + query.toString()
				+ " y ordenación " /* + sort.toString() */);

		Integer size = getCount(QueryBuilders.boolQuery().filter(query));

		SearchRequestBuilder requestBuilder = ESProvider.getClient().prepareSearch(getIndex()).setTypes(getType())
				.setQuery(query).setSize(size).addSort(sort);

		if (returnFields != null && returnFields.size() > 0) {
			requestBuilder.setFetchSource(ElasticSearchUtils.getReturnFields(returnFields), null);
		}
		return requestBuilder.execute().actionGet();
	}

	protected SearchResponse searchRequest(TQueryDTO queryDTO) {

		return searchRequest(queryDTO, null);
	}

	protected SearchResponse searchRequest(TQueryDTO queryDTO, QueryBuilder serviceQuery) {

		LOGGER.debug("Find en " + getIndex() + " " + getType() + " con queryDTO " + queryDTO + " y query interna ");

		SearchRequestBuilder requestBuilder = searchRequestBuilder(queryDTO, serviceQuery);

		return requestBuilder.execute().actionGet();
	}

	protected SearchRequestBuilder searchRequestBuilder(TQueryDTO queryDTO) {

		return searchRequestBuilder(queryDTO, null);
	}

	protected SearchRequestBuilder searchRequestBuilder(TQueryDTO queryDTO, QueryBuilder serviceQuery) {

		LOGGER.debug("Find en " + getIndex() + " " + getType() + " con queryDTO " + queryDTO + " y query interna ");

		SearchRequestBuilder requestBuilder = ESProvider.getClient().prepareSearch(getIndex()).setTypes(getType());

		QueryBuilder termQuery = getTermQuery(queryDTO.getTerms());

		BoolQueryBuilder partialQuery = null;
		if (serviceQuery != null) {
			partialQuery = QueryBuilders.boolQuery().must(serviceQuery);
		}

		if (termQuery != null && partialQuery != null) {
			partialQuery.must(termQuery);
		} else if (termQuery != null) {
			partialQuery = QueryBuilders.boolQuery().must(termQuery);
		}

		BoolQueryBuilder queryBuilder = getQuery(queryDTO, getInternalQuery(), partialQuery);

		requestBuilder.setQuery(queryBuilder);

		QueryBuilder postFilter = getPostFilter(queryDTO.getPostFilter());

		if (postFilter != null) {
			requestBuilder.setPostFilter(postFilter);
		}

		List<BaseAggregationBuilder> aggs = getAggs(queryDTO.getAggs());

		if (aggs != null) {
			for (BaseAggregationBuilder term : aggs) {
				requestBuilder.addAggregation((AggregationBuilder) term);
			}
		}

		if (queryDTO.getText() != null && (queryDTO.getText().getHighlightFields() == null
				|| queryDTO.getText().getHighlightFields().length == 0)) {

			requestBuilder.highlighter(getHighlightBuilder(queryDTO.getText().getHighlightFields()));
		}
		requestBuilder.setFrom(queryDTO.getFrom());
		requestBuilder.setSize(getSize(queryDTO, queryBuilder));

		List<SortBuilder<?>> sorts = getSorts(queryDTO.getSorts());
		// Finalmente solo en caso de tener una ordenación se añade a la request
		if (sorts != null && sorts.size() > 0) {
			for (int i = 0; i < sorts.size(); i++)
				requestBuilder.addSort(sorts.get(i));
		}

		List<String> returnFields = queryDTO.getReturnFields();
		if (returnFields != null && returnFields.size() > 0) {
			requestBuilder.setFetchSource(ElasticSearchUtils.getReturnFields(returnFields), null);
		}

		return requestBuilder;
	}

	private List<SortBuilder<?>> getSorts(List<SortDTO> sortDTOList) {

		// Se obtiene la ordenación enviada desde el exterior
		List<SortBuilder<?>> sorts = ElasticSearchUtils.getSorts(sortDTOList);
		// Si no existe, se establece la de por defecto del servicio en caso de
		// tenerla
		if (sorts == null) {
			return getSort();
		}
		return sorts;
	}

	private HighlightBuilder getHighlightBuilder(String[] highlightFields) {

		HighlightBuilder highlightBuilder = new HighlightBuilder();

		for (String field : highlightFields) {
			highlightBuilder.field(field);
		}

		highlightBuilder.preTags(PRETAGS);
		highlightBuilder.postTags(POSTTAGS);

		return highlightBuilder;
	}

	protected MultiSearchResponse getMultiFindResponses(List<SearchRequestBuilder> searchs) {

		MultiSearchRequestBuilder sr = ESProvider.getClient().prepareMultiSearch();

		for (int i = 0; i < searchs.size(); i++) {
			sr.add(searchs.get(i));
		}
		return sr.get();
	}

	protected List<?> scrollQueryReturnItems(QueryBuilder builder, IProcessItemFunction<?> func) {
		List<SortBuilder<?>> sortBuilders = getSort();
		SearchRequestBuilder query = ESProvider.getClient().prepareSearch(getIndex()).setTypes(getType())
				.setScroll(new TimeValue(60000)).setQuery(builder);

		for (SortBuilder<?> sort : sortBuilders) {
			query.addSort(sort);
		}

		SearchResponse scrollResp = query.setSize(100).execute().actionGet();
		while (true) {

			for (SearchHit hit : scrollResp.getHits().getHits()) {
				func.process(hit);
			}
			scrollResp = ESProvider.getClient().prepareSearchScroll(scrollResp.getScrollId())
					.setScroll(new TimeValue(600000)).execute().actionGet();

			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}

		return func.getResults();
	}

	/**
	 * Función que sirve de wrapper a la llamada de la util para crear la query de
	 * elastic.
	 * 
	 * @param queryDTO
	 *            Dto de la query enviada por el cliente.
	 * @param internalQuery
	 *            Query seteada en el repositorio y que siempre se aplicará.
	 * @param partialQuery
	 *            Query construida a partir de términos y queries que se definen en
	 *            el repositorio específico
	 * @return query de elastic.
	 */

	// TODO: sobrescribir cuando sea necesario crear queries específicas.
	protected BoolQueryBuilder getQuery(TQueryDTO queryDTO, QueryBuilder internalQuery, QueryBuilder partialQuery) {
		return SimpleQueryUtils.getQuery(queryDTO, getInternalQuery(), partialQuery);
	}

	/**
	 * Función que sirve de wrapper a la llamada de la util para crear el postfilter
	 * de elastic
	 * 
	 * @param postFilter
	 *            información para crear el postfilter. Es enviada desde el cliente.
	 * @return postFilter de elastic.
	 */
	protected QueryBuilder getPostFilter(Map<String, String[]> postFilter) {
		return ElasticSearchUtils.getPostFilter(postFilter);
	}

	/**
	 * Función que sirve de wrapper a la llamada de la util para crear las
	 * agregaciones.
	 * 
	 * @param queryDTO
	 *            Clase enviada desde el cliente con los datos necesarios para crear
	 *            la query.
	 * @return aggs de elastic.
	 */

	protected List<BaseAggregationBuilder> getAggs(List<AggsPropertiesDTO> aggs) {
		return ElasticSearchUtils.getAggs(aggs);
	}

	/**
	 * Función que devuelve el número total de elementos que se dan como resultado
	 * de aplicar todas las queries activas en el repo.
	 * 
	 * @param queryBuilder
	 *            Query completamente construida
	 * @return total de hits.
	 */

	public Integer getCount(BoolQueryBuilder queryBuilder) {

		SearchRequestBuilder queryCount = ESProvider.getClient().prepareSearch(INDEX).setTypes(TYPE).setSize(0);
		if (queryBuilder != null)
			queryCount.setQuery(queryBuilder);

		long result = queryCount.execute().actionGet().getHits().getTotalHits();

		return (int) result;
	}

	/**
	 * Función que nos devuelve el size de la query. El size del exterior tiene
	 * preferencia, en caso de no exista, se devuelve todo lo almacenado.
	 * 
	 * @param queryDTO.
	 *            queryDto para obtener parámetros de query enviados por el cliente
	 * @param query.
	 *            Query generada para en caso de que no se establezca size, calcular
	 *            el máximo
	 * 
	 * @return numero de elementos que devolverá la query
	 */

	protected Integer getSize(TQueryDTO queryDTO, BoolQueryBuilder query) {

		Integer size = queryDTO.getSize();
		if (size == null) {
			size = getCount(query);
			if (size > MAX_SIZE)
				return MAX_SIZE;
		}
		return size;
	}

	/**
	 * Función que nos devuelve una lista de ordenaciones específica para
	 * timeseries. Por defecto, ordena por id.
	 * 
	 * @return lista de ordenaciones de elasticsearch
	 */

	protected List<SortBuilder<?>> getSort() {

		List<SortBuilder<?>> sorts = new ArrayList<SortBuilder<?>>();
		sorts.add(SortBuilders.scoreSort().order(SortOrder.DESC));
		return sorts;
	}

	/**
	 * Función que dado un conjunto de términos, nos devuelve una query de
	 * elasticsearch. Debe estar implementado en cada repositorio para darle una
	 * funcionalidad específica y aquí estarán las funcionalidades que comparten
	 * todos los repositorios.
	 * 
	 * @param terms
	 *            Map de términos pasados por la query.
	 * @return query de tipo terms de elasticsearch.
	 */
	protected QueryBuilder getTermQuery(Map<String, Object> terms) {
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		return getTermQuery(terms, query);
	}

	/**
	 * Función que dado un conjunto de términos, nos devuelve una query de
	 * elasticsearch. Debe estar implementado en cada repositorio para darle una
	 * funcionalidad específica y aquí estarán las funcionalidades que comparten
	 * todos los repositorios.
	 * 
	 * @param terms
	 *            Map de términos pasados por la query.
	 * @param query
	 *            QueryBuilder con la query de los términos acumulados en los
	 *            repositorios específicos.
	 * @return query de tipo terms de elasticsearch.
	 */

	@SuppressWarnings("unchecked")
	protected QueryBuilder getTermQuery(Map<String, Object> terms, BoolQueryBuilder query) {

		if (terms != null && terms.containsKey("selection")) {
			String selectedId = (String) terms.get("selection");

			Map<String, Object> selected = findSelectedItems(selectedId);

			if (selected != null) {
				List<String> ids = (List<String>) (List<?>) selected.get("ids");
				query.must(QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery("id", ids)));
			}
		}
		return query.hasClauses() ? query : null;
	}

	/**
	 * Función que dado un id de selección devuelve todos los items seleccionados.
	 * 
	 * @param selectionId
	 *            identificador de la selección con la que se está trabajando.
	 * @return Map de identificadores de items seleccionados.
	 */
	protected Map<String, Object> findSelectedItems(String selectionId) {

		/*
		 * String selectionIndex = SelectionWorkRepository.INDEX[0]; String
		 * selectionType = SelectionWorkRepository.TYPE[0];
		 * 
		 * GetResponse result = ESProvider.getClient().prepareGet(selectionIndex,
		 * selectionType, selectionId.toString()) .execute().actionGet(); if
		 * (result.isExists()) return result.getSource();
		 **/
		return null;
	}

	/**
	 * Función para setear una query que siempre se aplicará para el repositorio.
	 * 
	 * @param internalQuery
	 *            Query de tipo elasticSearch.
	 */

	protected void setInternalQuery(QueryBuilder internalQuery) {
		this.INTERNAL_QUERY = internalQuery;
	}

	/**
	 * Función para obtener la query de tipo elasticSearch que se debe aplicar
	 * siempre.
	 * 
	 * @return Query de tipo elasticSearch.
	 */
	protected QueryBuilder getInternalQuery() {
		return this.INTERNAL_QUERY;
	}

	@SuppressWarnings("unchecked")
	protected List<TModel> findWithNestedReference(String field, String termFilter, Long id) {

		QueryBuilder builder = QueryBuilders.boolQuery().filter(QueryBuilders.nestedQuery(field,
				QueryBuilders.boolQuery().must(QueryBuilders.termQuery(termFilter, id)), ScoreMode.Avg));

		return ((List<TModel>) scrollQueryReturnItems(builder));
	}

	@SuppressWarnings("unchecked")
	protected List<TModel> findWithSpecificReference(String termFilter, Long id) {

		QueryBuilder builder = QueryBuilders.boolQuery()
				.filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery(termFilter, id)));

		return ((List<TModel>) scrollQueryReturnItems(builder));
	}

	protected abstract List<?> scrollQueryReturnItems(QueryBuilder builder);

	protected <W> W getResponseToWrapper(GetResponse response, JavaType wrapperType) {

		return objectMapper.convertValue(ElasticSearchUtils.getResponsetoObject(response), wrapperType);
	}

	protected <W> W searchResponseToWrapper(SearchResponse response, JavaType wrapperType) {

		return objectMapper.convertValue(ElasticSearchUtils.searchResponsetoObject(response), wrapperType);
	}

	@SuppressWarnings("unchecked")
	protected <W> W mgetResponseToWrapper(MultiGetResponse result, JavaType wrapperType) {

		return (W) ElasticSearchUtils.parseMGetHit(result, wrapperType);
	}

	public String[] getIndex() {
		return INDEX;
	}

	public String[] getType() {
		return TYPE;
	}
}