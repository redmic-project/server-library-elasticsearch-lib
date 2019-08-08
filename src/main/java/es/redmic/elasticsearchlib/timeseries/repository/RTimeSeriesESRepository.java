package es.redmic.elasticsearchlib.timeseries.repository;

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
import org.springframework.beans.factory.annotation.Value;

import es.redmic.elasticsearchlib.data.repository.RDataESRepository;
import es.redmic.models.es.common.model.BaseTimeDataAbstractES;
import es.redmic.models.es.common.query.dto.DataQueryDTO;

public abstract class RTimeSeriesESRepository<TModel extends BaseTimeDataAbstractES, TQueryDTO extends DataQueryDTO>
		extends RDataESRepository<TModel, TQueryDTO> implements IBaseTimeSeriesESRepository {

	@Value("${timeseries.index.pattern}")
	String timeSeriesIndexPattern;

	public RTimeSeriesESRepository() {
		super(IBaseTimeSeriesESRepository.INDEX, IBaseTimeSeriesESRepository.TYPE, true);
	}

	@Override
	protected String getIndex(final TModel modelToIndex) {
		return getIndex()[0] + "-" + modelToIndex.getDate().toString(DateTimeFormat.forPattern(timeSeriesIndexPattern));
	}
}
