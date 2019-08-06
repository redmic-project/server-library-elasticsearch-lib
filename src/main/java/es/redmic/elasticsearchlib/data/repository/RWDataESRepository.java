package es.redmic.elasticsearchlib.data.repository;

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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.springframework.beans.factory.annotation.Autowired;

import es.redmic.elasticsearchlib.common.repository.IRWBaseESRepository;
import es.redmic.elasticsearchlib.common.utils.ElasticPersistenceUtils;
import es.redmic.models.es.common.dto.EventApplicationResult;
import es.redmic.models.es.common.model.BaseES;
import es.redmic.models.es.common.query.dto.SimpleQueryDTO;

public abstract class RWDataESRepository<TModel extends BaseES<?>, TQueryDTO extends SimpleQueryDTO>
		extends RDataESRepository<TModel, TQueryDTO> implements IRWBaseESRepository<TModel> {

	@Autowired
	ElasticPersistenceUtils elasticPersistenceUtils;

	public RWDataESRepository() {
		super();
	}

	public RWDataESRepository(String[] index, String type) {
		super(index, type);
	}

	public RWDataESRepository(String[] index, String type, Boolean rollOverIndex) {
		super(index, type, rollOverIndex);
	}

	@Override
	public EventApplicationResult save(TModel modelToIndex) {

		return save(modelToIndex, null);
	}

	@Override
	public EventApplicationResult save(TModel modelToIndex, String parentId) {

		EventApplicationResult checkInsert = checkInsertConstraintsFulfilled(modelToIndex);

		if (!checkInsert.isSuccess()) {
			return checkInsert;
		}

		return elasticPersistenceUtils.save(getIndex(modelToIndex), getType(), modelToIndex,
				modelToIndex.getId().toString(), parentId);
	}

	@Override
	public EventApplicationResult update(TModel modelToIndex) {

		return update(modelToIndex, null);
	}

	@Override
	public EventApplicationResult update(TModel modelToIndex, String parentId) {

		EventApplicationResult checkUpdate = checkUpdateConstraintsFulfilled(modelToIndex);

		if (!checkUpdate.isSuccess()) {
			return checkUpdate;
		}

		return elasticPersistenceUtils.update(getIndex(modelToIndex), getType(), modelToIndex,
				modelToIndex.getId().toString(), parentId);
	}

	@Override
	public EventApplicationResult update(String id, XContentBuilder doc) {

		return update(id, null, doc);
	}

	@Override
	public EventApplicationResult update(String id, String parentId, XContentBuilder doc) {

		return elasticPersistenceUtils.update(getIndex()[0], getType(), id, parentId, doc);
	}

	@Override
	public EventApplicationResult delete(String id) {

		return delete(id, null);
	}

	@Override
	public EventApplicationResult delete(String id, String parentId) {

		EventApplicationResult checkDelete = checkDeleteConstraintsFulfilled(id);

		if (!checkDelete.isSuccess()) {
			return checkDelete;
		}

		return elasticPersistenceUtils.delete(getIndex()[0], getType(), id, parentId);
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
