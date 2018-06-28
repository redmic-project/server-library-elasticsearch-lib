package es.redmic.elasticsearchlib.data.repository;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.search.SearchHit;

import com.fasterxml.jackson.databind.ObjectMapper;

import es.redmic.elasticsearchlib.common.utils.IProcessItemFunction;
import es.redmic.models.es.common.model.BaseES;

public class DataItemsProcessingFunction<TModel extends BaseES<?>> implements IProcessItemFunction<TModel> {

	List<TModel> items = new ArrayList<TModel>();
	Class<TModel> typeOfTModel;
	protected ObjectMapper objectMapper;

	public DataItemsProcessingFunction(Class<TModel> typeOfTModel, ObjectMapper objectMapper) {
		this.typeOfTModel = typeOfTModel;
		this.objectMapper = objectMapper;
	}

	@Override
	public void process(SearchHit hit) {
		TModel item = mapper(hit);
		items.add(item);
	}

	private TModel mapper(SearchHit hit) {

		return objectMapper.convertValue(hit.getSource(), this.typeOfTModel);
	}

	@Override
	public List<?> getResults() {
		return items;
	}
}