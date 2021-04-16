package es.redmic.elasticsearchlib.series.repository;

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

import org.joda.time.format.DateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import es.redmic.elasticsearchlib.common.utils.ElasticPersistenceUtils;
import es.redmic.models.es.common.dto.EventApplicationResult;
import es.redmic.models.es.common.model.BaseTimeDataAbstractES;
import es.redmic.models.es.common.query.dto.DataQueryDTO;

public abstract class RWSeriesESRepository<TModel extends BaseTimeDataAbstractES, TQueryDTO extends DataQueryDTO>
		extends RSeriesESRepository<TModel, TQueryDTO> implements IRWSeriesESRepository<TModel> {

	@Value("${timeseries.index.pattern}")
	String timeSeriesIndexPattern;

	@Autowired
	ElasticPersistenceUtils elasticPersistenceUtils;

	protected RWSeriesESRepository() {
		super();
	}

	@Override
	protected String getIndex(final TModel modelToIndex) {
		return getIndex()[0] + "-" + modelToIndex.getDate().toString(DateTimeFormat.forPattern(timeSeriesIndexPattern));
	}

	@Override
	public EventApplicationResult save(TModel modelToIndex) {

		EventApplicationResult checkInsert = checkInsertConstraintsFulfilled(modelToIndex);

		if (!checkInsert.isSuccess()) {
			return checkInsert;
		}

		return elasticPersistenceUtils.save(getIndex(modelToIndex), getType(), modelToIndex,
				modelToIndex.getId());
	}

	@Override
	public EventApplicationResult update(TModel modelToIndex) {

		EventApplicationResult checkUpdate = checkUpdateConstraintsFulfilled(modelToIndex);

		if (!checkUpdate.isSuccess()) {
			return checkUpdate;
		}

		return elasticPersistenceUtils.update(getIndex(modelToIndex), getType(), modelToIndex,
				modelToIndex.getId());
	}

	@Override
	public EventApplicationResult delete(String id) {

		EventApplicationResult checkDelete = checkDeleteConstraintsFulfilled(id);

		if (!checkDelete.isSuccess()) {
			return checkDelete;
		}

		return elasticPersistenceUtils.delete(getIndex()[0], getType(), id);
	}

	private EventApplicationResult checkUpdateConstraintsFulfilled(TModel modelToIndex) {
		// TODO Implementar comprobaciones
		return new EventApplicationResult(true);
	}

	private EventApplicationResult checkDeleteConstraintsFulfilled(String id) {
		// TODO Implementar comprobaciones
		return new EventApplicationResult(true);
	}

	private EventApplicationResult checkInsertConstraintsFulfilled(TModel modelToIndex) {
		// TODO Implementar comprobaciones
		return new EventApplicationResult(true);
	}
}
