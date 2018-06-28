package es.redmic.elasticsearchlib.data.repository;

import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;

import es.redmic.elasticsearchlib.common.utils.ElasticPersistenceUtils;
import es.redmic.exception.common.ExceptionType;
import es.redmic.models.es.common.dto.EventApplicationResult;
import es.redmic.models.es.common.model.BaseES;
import es.redmic.models.es.common.query.dto.SimpleQueryDTO;

public abstract class RWDataESRepository<TModel extends BaseES<?>, TQueryDTO extends SimpleQueryDTO>
		extends RDataESRepository<TModel, TQueryDTO> {

	@Autowired
	ElasticPersistenceUtils<TModel> elasticPersistenceUtils;

	public RWDataESRepository() {
		super();
	}

	public RWDataESRepository(String[] index, String[] type) {
		super(index, type);
	}

	public EventApplicationResult save(TModel modelToIndex) {

		EventApplicationResult checkInsert = checkInsertConstraintsFulfilled(modelToIndex);

		if (!checkInsert.isSuccess()) {
			return checkInsert;
		}

		// @formatter:off

		IndexResponse result = ESProvider.getClient()
			.prepareIndex(getIndex()[0], getType()[0])
			.setSource(convertTModelToSource(modelToIndex))
			.setId((modelToIndex.getId() != null) ? modelToIndex.getId().toString() : null)
			.setRefreshPolicy(RefreshPolicy.IMMEDIATE)
				.execute()
					.actionGet();

		// @formatter:on

		if (!result.status().equals(RestStatus.CREATED)) {
			LOGGER.debug("Error indexando en " + getIndex()[0] + " " + getType()[0]);
			return new EventApplicationResult(ExceptionType.ES_INDEX_DOCUMENT.toString());
		}

		return new EventApplicationResult(true);
	}

	public EventApplicationResult update(TModel modelToIndex) {

		EventApplicationResult checkUpdate = checkUpdateConstraintsFulfilled(modelToIndex);

		if (!checkUpdate.isSuccess()) {
			return checkUpdate;
		}

		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		updateRequest.index(getIndex()[0]);
		updateRequest.type(getType()[0]);
		updateRequest.id(modelToIndex.getId().toString());
		updateRequest.doc(convertTModelToSource(modelToIndex));
		updateRequest.fetchSource(true);

		try {
			ESProvider.getClient().update(updateRequest).get();
		} catch (InterruptedException | ExecutionException e) {
			LOGGER.debug("Error modificando el item con id " + modelToIndex.getId() + " en " + getIndex()[0] + " "
					+ getType()[0]);
			return new EventApplicationResult(ExceptionType.ES_UPDATE_DOCUMENT.toString());
		}

		return new EventApplicationResult(true);
	}

	public EventApplicationResult delete(String id) {

		EventApplicationResult checkDelete = checkDeleteConstraintsFulfilled(id);

		if (!checkDelete.isSuccess()) {
			return checkDelete;
		}

		// @formatter:off

		DeleteResponse result = ESProvider.getClient()
			.prepareDelete(getIndex()[0], getType()[0], id)
			.setRefreshPolicy(RefreshPolicy.IMMEDIATE)
				.execute()
					.actionGet();

		// @formatter:on

		if (!result.status().equals(RestStatus.OK)) {
			LOGGER.debug("Error borrando el item con id " + id + " en " + getIndex()[0] + " " + getType()[0]);
			return new EventApplicationResult(ExceptionType.DELETE_ITEM_EXCEPTION.toString());
		}
		return new EventApplicationResult(true);
	}

	/*
	 * Función que comprueba que las restricciones necesarias para añadir el item se
	 * cumplen. Por ejemplo que no exista el identificador, que no haya códigos
	 * repetidos. En caso de no cumplirse devuelve la excepción etc
	 */
	protected abstract EventApplicationResult checkInsertConstraintsFulfilled(TModel modelToIndex);

	/*
	 * Función que comprueba que las restricciones necesarias para editar el item se
	 * cumplen. Por ejemplo que no haya códigos repetidos, etc
	 */
	protected abstract EventApplicationResult checkUpdateConstraintsFulfilled(TModel modelToIndex);

	/*
	 * Función que comprueba que las restricciones necesarias para borrar el item se
	 * cumplen. Por ejemplo que no esté referenciado en otros servicios.
	 */
	protected abstract EventApplicationResult checkDeleteConstraintsFulfilled(String modelToIndex);
}
