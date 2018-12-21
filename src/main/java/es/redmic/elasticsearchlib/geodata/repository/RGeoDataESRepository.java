package es.redmic.elasticsearchlib.geodata.repository;

import java.util.List;

import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;

import com.fasterxml.jackson.databind.JavaType;

import es.redmic.elasticsearchlib.common.repository.RBaseESRepository;
import es.redmic.models.es.common.query.dto.MgetDTO;
import es.redmic.models.es.common.query.dto.SimpleQueryDTO;
import es.redmic.models.es.geojson.base.Feature;
import es.redmic.models.es.geojson.wrapper.GeoHitWrapper;
import es.redmic.models.es.geojson.wrapper.GeoHitsWrapper;
import es.redmic.models.es.geojson.wrapper.GeoSearchWrapper;

public abstract class RGeoDataESRepository<TModel extends Feature<?, ?>, TQueryDTO extends SimpleQueryDTO>
		extends RBaseESRepository<TModel, TQueryDTO> {

	public RGeoDataESRepository(String[] index, String[] type, Boolean rollOverIndex) {
		super(index, type, rollOverIndex);
	}

	public RGeoDataESRepository(String[] index, String[] type) {
		super(index, type);
	}

	public GeoHitWrapper<?> findById(String id) {

		return getResponseToWrapper(getRequest(id), getSourceType(GeoHitWrapper.class));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public GeoHitsWrapper<?> mget(MgetDTO dto) {

		MultiGetResponse result = multigetRequest(dto);

		List<GeoHitWrapper<?>> hits = mgetResponseToWrapper(result, getSourceType(GeoHitWrapper.class));
		GeoHitsWrapper data = new GeoHitsWrapper(hits);
		data.setTotal(hits.size());
		return data;
	}

	protected GeoSearchWrapper<?> findBy(QueryBuilder queryBuilder) {

		return findBy(queryBuilder, null);
	}

	protected GeoSearchWrapper<?> findBy(QueryBuilder queryBuilder, List<String> returnFields) {

		return searchResponseToWrapper(searchRequest(queryBuilder, returnFields),
				getSourceType(GeoSearchWrapper.class));
	}

	public GeoSearchWrapper<?> find(TQueryDTO queryDTO) {

		SearchResponse result = super.searchRequest(queryDTO);

		return searchResponseToWrapper(result, getSourceType(GeoSearchWrapper.class));
	}

	@Override
	protected List<?> scrollQueryReturnItems(QueryBuilder builder) {

		return scrollQueryReturnItems(builder, new GeoItemsProcessingFunction<TModel>(typeOfTModel, objectMapper));
	}

	public SimpleQueryDTO createSimpleQueryDTOFromTextQueryParams(Integer from, Integer size) {

		SimpleQueryDTO queryDTO = new SimpleQueryDTO();

		if (from != null)
			queryDTO.setFrom(from);
		if (size != null)
			queryDTO.setSize(size);

		return queryDTO;
	}

	@Override
	protected JavaType getSourceType(Class<?> wrapperClass) {
		return objectMapper.getTypeFactory().constructParametricType(wrapperClass, typeOfTModel);
	}
}
