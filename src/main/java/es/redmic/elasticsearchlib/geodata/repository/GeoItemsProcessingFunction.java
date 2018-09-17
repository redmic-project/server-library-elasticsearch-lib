package es.redmic.elasticsearchlib.geodata.repository;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import com.fasterxml.jackson.databind.ObjectMapper;

import es.redmic.elasticsearchlib.common.utils.IProcessItemFunction;
import es.redmic.models.es.geojson.common.model.Feature;

public class GeoItemsProcessingFunction<TModel extends Feature<?, ?>> implements IProcessItemFunction<TModel> {

	List<TModel> items = new ArrayList<TModel>();
	Class<TModel> typeOfTModel;
	protected ObjectMapper objectMapper;

	public GeoItemsProcessingFunction(Class<TModel> typeOfTModel, ObjectMapper objectMapper) {

		this.typeOfTModel = typeOfTModel;
		this.objectMapper = objectMapper;
	}

	@Override
	public void process(SearchHit hit) {

		TModel item = mapper(hit);
		items.add(item);
	}

	private TModel mapper(SearchHit hit) {

		TModel item = objectMapper.convertValue(hit.getSourceAsMap(), this.typeOfTModel);
		SearchHitField parent = hit.getFields().get("_parent");
		item.set_parentId(parent.getValue().toString());

		return item;
	}

	@Override
	public List<?> getResults() {
		return items;
	}
}