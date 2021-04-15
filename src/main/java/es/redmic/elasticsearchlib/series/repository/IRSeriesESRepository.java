package es.redmic.elasticsearchlib.series.repository;

/*-
 * #%L
 * elasticsearch-lib
 * %%
 * Copyright (C) 2019 - 2021 REDMIC Project / Server
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

import java.util.List;

import org.elasticsearch.search.builder.SearchSourceBuilder;

import es.redmic.models.es.common.model.BaseAbstractStringES;
import es.redmic.models.es.common.query.dto.DataQueryDTO;
import es.redmic.models.es.common.query.dto.MgetDTO;
import es.redmic.models.es.series.common.model.SeriesHitWrapper;
import es.redmic.models.es.series.common.model.SeriesHitsWrapper;
import es.redmic.models.es.series.common.model.SeriesSearchWrapper;

public interface IRSeriesESRepository<TModel extends BaseAbstractStringES> extends IBaseTimeSeriesESRepository {

	public SeriesHitWrapper<TModel> findById(String id);
	public SeriesSearchWrapper<TModel> searchByIds(String[] ids);
	public SeriesSearchWrapper<TModel> find(DataQueryDTO queryDTO);
	public List<SeriesSearchWrapper<TModel>> multiFind(List<SearchSourceBuilder> searchs);
	public SeriesHitsWrapper<TModel> mget(MgetDTO dto);
}
