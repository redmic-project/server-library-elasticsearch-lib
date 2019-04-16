package es.redmic.elasticsearchlib.common.utils;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import es.redmic.elasticsearchlib.config.EsClientProvider;
import es.redmic.exception.common.ExceptionType;
import es.redmic.exception.elasticsearch.ESUpdateException;
import es.redmic.models.es.common.dto.EventApplicationResult;
import es.redmic.models.es.common.model.BaseES;

@Component
public class ElasticPersistenceUtils<TModel extends BaseES<?>> {

	protected static Logger logger = LogManager.getLogger();

	@Autowired
	EsClientProvider ESProvider;

	@Autowired
	protected ObjectMapper objectMapper;

	protected static String SCRIPT_ENGINE = "groovy";

	public EventApplicationResult save(String index, String type, TModel model, String id) {

		// @formatter:off
		
		IndexRequest request = new IndexRequest(index, type)
				.source(convertTModelToSource(model))
				.id(id)
				.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		
		// @formatter:on

		try {
			ESProvider.getClient().index(request, RequestOptions.DEFAULT);
			return new EventApplicationResult(true);
		} catch (IOException e) {
			logger.debug("Error indexando en " + index + " " + type);
			e.printStackTrace();
			return new EventApplicationResult(ExceptionType.ES_INDEX_DOCUMENT.toString());
		}
	}

	public EventApplicationResult update(String index, String type, TModel model, String id) {

		// @formatter:off

		UpdateRequest updateRequest = new UpdateRequest(index, type, id)
				.doc(convertTModelToSource(model))
				.fetchSource(false)
				.setRefreshPolicy(RefreshPolicy.IMMEDIATE);

		// @formatter:on

		try {
			ESProvider.getClient().update(updateRequest, RequestOptions.DEFAULT);

			return new EventApplicationResult(true);
		} catch (IOException e) {
			logger.debug("Error modificando el item con id " + id + " en " + index + " " + type);
			e.printStackTrace();
			return new EventApplicationResult(ExceptionType.ES_UPDATE_DOCUMENT.toString());
		}

	}

	public EventApplicationResult update(String index, String type, String id, XContentBuilder doc) {

		// @formatter:off
		
		UpdateRequest updateRequest = new UpdateRequest(index, type, id)
				.setRefreshPolicy(RefreshPolicy.IMMEDIATE)
				.doc(doc)
				.fetchSource(true);
		
		// @formatter:on

		try {
			ESProvider.getClient().update(updateRequest, RequestOptions.DEFAULT);
		} catch (Exception e) {
			logger.debug("Error modificando el item con id " + id + " en " + index + " " + type);
			return new EventApplicationResult(ExceptionType.ES_UPDATE_DOCUMENT.toString());
		}

		return new EventApplicationResult(true);
	}

	public EventApplicationResult delete(String index, String type, String id) {

		DeleteRequest deleteRequest = new DeleteRequest(index, type, id).setRefreshPolicy(RefreshPolicy.IMMEDIATE);

		try {
			ESProvider.getClient().delete(deleteRequest, RequestOptions.DEFAULT);
			return new EventApplicationResult(true);
		} catch (IOException e) {
			logger.debug("Error borrando el item con id " + id + " en " + index + " " + type);
			return new EventApplicationResult(ExceptionType.DELETE_ITEM_EXCEPTION.toString());
		}
	}

	@SuppressWarnings("unchecked")
	protected Map<String, Object> convertTModelToSource(TModel modelToIndex) {
		return objectMapper.convertValue(modelToIndex, Map.class);
	}

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

	public List<UpdateResponse> updateByBulk(List<UpdateRequest> listUpdates) {

		BulkRequest bulkRequest = new BulkRequest();

		for (int i = 0; i < listUpdates.size(); i++)
			bulkRequest.add(listUpdates.get(i));
		bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);

		BulkResponse bulkResponse;
		try {
			bulkResponse = ESProvider.getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ESUpdateException("Error ejecutando modificación en batch");
		}

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

		BulkRequest bulkRequest = new BulkRequest();

		for (int i = 0; i < listIndexs.size(); i++)
			bulkRequest.add(listIndexs.get(i));
		bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);

		BulkResponse bulkResponse;
		try {
			bulkResponse = ESProvider.getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ESUpdateException("Error ejecutando indexación en batch");
		}

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
