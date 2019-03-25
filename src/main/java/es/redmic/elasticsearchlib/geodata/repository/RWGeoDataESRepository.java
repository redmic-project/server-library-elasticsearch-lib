package es.redmic.elasticsearchlib.geodata.repository;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.springframework.beans.factory.annotation.Autowired;

import es.redmic.elasticsearchlib.common.repository.IRWBaseESRepository;
import es.redmic.elasticsearchlib.common.utils.ElasticPersistenceUtils;
import es.redmic.models.es.common.dto.EventApplicationResult;
import es.redmic.models.es.common.query.dto.SimpleQueryDTO;
import es.redmic.models.es.geojson.base.Feature;

public abstract class RWGeoDataESRepository<TModel extends Feature<?, ?>, TQueryDTO extends SimpleQueryDTO>
		extends RGeoDataESRepository<TModel, TQueryDTO> implements IRWBaseESRepository<TModel> {

	@Autowired
	ElasticPersistenceUtils<TModel> elasticPersistenceUtils;

	public RWGeoDataESRepository(String[] index, String type, Boolean rollOverIndex) {
		super(index, type, rollOverIndex);
	}

	public RWGeoDataESRepository(String[] index, String type) {
		super(index, type);
	}

	@Override
	public EventApplicationResult save(TModel modelToIndex) {

		EventApplicationResult checkInsert = checkInsertConstraintsFulfilled(modelToIndex);

		if (!checkInsert.isSuccess()) {
			return checkInsert;
		}

		return elasticPersistenceUtils.save(getIndex(modelToIndex), getType(), modelToIndex, modelToIndex.getId());
	}

	@Override
	public EventApplicationResult update(TModel modelToIndex) {

		EventApplicationResult checkUpdate = checkUpdateConstraintsFulfilled(modelToIndex);

		if (!checkUpdate.isSuccess()) {
			return checkUpdate;
		}

		return elasticPersistenceUtils.update(getIndex(modelToIndex), getType(), modelToIndex,
				modelToIndex.getId().toString());
	}

	@Override
	public EventApplicationResult update(String id, XContentBuilder doc) {

		return elasticPersistenceUtils.update(getIndex()[0], getType(), id, doc);
	}

	@Override
	public EventApplicationResult delete(String id) {

		EventApplicationResult checkDelete = checkDeleteConstraintsFulfilled(id);

		if (!checkDelete.isSuccess()) {
			return checkDelete;
		}

		return elasticPersistenceUtils.delete(getIndex()[0], getType(), id);
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