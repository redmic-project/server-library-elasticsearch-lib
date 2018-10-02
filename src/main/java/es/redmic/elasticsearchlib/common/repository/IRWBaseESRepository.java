package es.redmic.elasticsearchlib.common.repository;

import org.elasticsearch.common.xcontent.XContentBuilder;

import es.redmic.models.es.common.dto.EventApplicationResult;
import es.redmic.models.es.common.model.BaseES;

public interface IRWBaseESRepository<TModel extends BaseES<?>> {

	EventApplicationResult save(TModel modelToIndex);

	EventApplicationResult update(TModel modelToIndex);

	EventApplicationResult update(String id, XContentBuilder doc);

	EventApplicationResult delete(String id);
}
