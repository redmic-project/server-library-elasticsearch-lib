package es.redmic.elasticsearchlib.common.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import es.redmic.elasticsearchlib.config.EsClientProvider;
import es.redmic.exception.elasticsearch.ESUpdateException;
import es.redmic.models.es.common.model.BaseES;

@Component
public class ElasticPersistenceUtils<TModel extends BaseES<?>> {

	@Autowired
	EsClientProvider ESProvider;

	protected static String SCRIPT_ENGINE = "groovy";

	public List<UpdateRequest> getUpdateRequest(String[] index, String[] type, String id, Map<String, Object> fields) {

		return getUpdateRequest(index, type, id, fields, null, null);
	}

	public List<UpdateRequest> getUpdateRequest(String[] index, String[] type, String id, Map<String, Object> fields,
			String parentId) {

		return getUpdateRequest(index, type, id, fields, parentId, null);
	}

	public List<UpdateRequest> getUpdateRequest(String[] index, String[] type, String id, Map<String, Object> fields,
			String parentId, String grandParentId) {

		List<UpdateRequest> result = new ArrayList<UpdateRequest>();

		for (int i = 0; i < index.length; i++) {
			for (int j = 0; j < type.length; j++) {
				UpdateRequest updateRequest = new UpdateRequest();
				updateRequest.index(index[i]);
				updateRequest.type(type[j]);
				updateRequest.id(id);
				updateRequest.fetchSource(true);
				if (parentId != null)
					updateRequest.parent(grandParentId);

				if (grandParentId != null)
					updateRequest.routing(grandParentId);

				updateRequest.doc(fields);
				result.add(updateRequest);
			}
		}
		return result;
	}

	public List<UpdateRequest> getUpdateScript(String[] index, String[] type, String id, Map<String, Object> fields,
			String scriptName) {

		return getUpdateScript(index, type, id, fields, scriptName, null, null);
	}

	public List<UpdateRequest> getUpdateScript(String[] index, String[] type, String id, Map<String, Object> fields,
			String scriptName, String parentId) {

		return getUpdateScript(index, type, id, fields, scriptName, parentId, null);
	}

	public List<UpdateRequest> getUpdateScript(String[] index, String[] type, String id, Map<String, Object> fields,
			String scriptName, String parentId, String grandParentId) {

		List<UpdateRequest> result = new ArrayList<UpdateRequest>();

		for (int i = 0; i < index.length; i++) {
			for (int j = 0; j < type.length; j++) {
				UpdateRequest updateRequest = new UpdateRequest();
				updateRequest.index(index[i]);
				updateRequest.type(type[j]);
				updateRequest.id(id);
				updateRequest.retryOnConflict(3);
				updateRequest.fetchSource(true);

				if (parentId != null)
					updateRequest.parent(parentId);

				if (grandParentId != null)
					updateRequest.routing(grandParentId);

				updateRequest.script(new Script(ScriptType.FILE, SCRIPT_ENGINE, scriptName, fields));
				updateRequest.retryOnConflict(2);
				result.add(updateRequest);
			}
		}
		return result;
	}

	public List<UpdateResponse> updateByBulk(List<UpdateRequest> listUpdates) {

		BulkRequestBuilder bulkRequest = ESProvider.getClient().prepareBulk();

		for (int i = 0; i < listUpdates.size(); i++)
			bulkRequest.add(listUpdates.get(i));
		bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);

		BulkResponse bulkResponse = bulkRequest.execute().actionGet();

		if (bulkResponse.hasFailures()) {

			String message = bulkResponse.buildFailureMessage();
			// TODO: Cambiar si no cambia estructura de document. Descarta error
			// controlado en document
			if (!message.contains("DocumentMissingException"))
				throw new ESUpdateException(message);
		}

		BulkItemResponse[] items = bulkResponse.getItems();
		List<UpdateResponse> result = new ArrayList<UpdateResponse>();
		for (int i = 0; i < items.length; i++) {
			UpdateResponse item = (UpdateResponse) items[i].getResponse();

			if (item != null && item.getGetResult().isExists())
				result.add(item);
		}
		return result;
	}

	public List<IndexResponse> indexByBulk(List<IndexRequest> listIndexs) {

		BulkRequestBuilder bulkRequest = ESProvider.getClient().prepareBulk();

		for (int i = 0; i < listIndexs.size(); i++)
			bulkRequest.add(listIndexs.get(i));
		bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();

		if (bulkResponse.hasFailures())
			throw new ESUpdateException(bulkResponse.buildFailureMessage());

		BulkItemResponse[] items = bulkResponse.getItems();
		List<IndexResponse> result = new ArrayList<IndexResponse>();
		for (int i = 0; i < items.length; i++) {
			IndexResponse item = items[i].getResponse();
			result.add(item);
		}
		return result;
	}
}
