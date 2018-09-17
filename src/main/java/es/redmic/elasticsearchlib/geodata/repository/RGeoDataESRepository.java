package es.redmic.elasticsearchlib.geodata.repository;

import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import com.fasterxml.jackson.databind.JavaType;
import com.vividsolutions.jts.geom.Geometry;

import es.redmic.elasticsearchlib.common.query.DataQueryUtils;
import es.redmic.elasticsearchlib.common.repository.RBaseESRepository;
import es.redmic.exception.data.ItemNotFoundException;
import es.redmic.models.es.common.query.dto.MgetDTO;
import es.redmic.models.es.common.query.dto.SimpleQueryDTO;
import es.redmic.models.es.geojson.common.model.Feature;
import es.redmic.models.es.geojson.common.model.GeoHitWrapper;
import es.redmic.models.es.geojson.common.model.GeoHitsWrapper;
import es.redmic.models.es.geojson.common.model.GeoSearchWrapper;
import es.redmic.models.es.geojson.properties.model.GeoDataProperties;

public abstract class RGeoDataESRepository<TModel extends Feature<?, ?>, TQueryDTO extends SimpleQueryDTO>
		extends RBaseESRepository<TModel, TQueryDTO> {

	public RGeoDataESRepository(String[] index, String[] type) {
		super(index, type);
	}

	/*
	 * Sobrescribe método base para añadir query de control de accesso a datos
	 */

	public GeoHitWrapper<?, ?> findById(String id) {

		BoolQueryBuilder query = DataQueryUtils.getItemsQuery(id);

		GeoSearchWrapper<?, ?> result = findBy(query);

		if (result.getHits() == null || result.getHits().getHits() == null || result.getHits().getHits().size() != 1)
			throw new ItemNotFoundException("id", id);

		return result.getHits().getHits().get(0);
	}

	public GeoHitsWrapper<?, ?> mget(MgetDTO dto) {

		List<String> ids = dto.getIds();

		GeoSearchWrapper<?, ?> result = findBy(DataQueryUtils.getItemsQuery(ids), dto.getFields());

		if (result.getHits() == null || result.getHits().getHits() == null)
			throw new ItemNotFoundException("ids", dto.getIds().toString());

		if (result.getHits().getHits().size() != ids.size()) {

			for (GeoHitWrapper<?, ?> hit : result.getHits().getHits()) {
				ids.remove(hit.get_id());
			}

			throw new ItemNotFoundException("ids", ids.toString());
		}

		return result.getHits();
	}

	protected GeoSearchWrapper<?, ?> findBy(QueryBuilder queryBuilder) {

		return findBy(queryBuilder, null);
	}

	protected GeoSearchWrapper<?, ?> findBy(QueryBuilder queryBuilder, List<String> returnFields) {

		return searchResponseToWrapper(searchRequest(queryBuilder, returnFields),
				getSourceType(GeoSearchWrapper.class));
	}

	public GeoSearchWrapper<?, ?> find(TQueryDTO queryDTO) {

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
		return objectMapper.getTypeFactory().constructParametricType(wrapperClass, GeoDataProperties.class,
				Geometry.class);
	}
}
