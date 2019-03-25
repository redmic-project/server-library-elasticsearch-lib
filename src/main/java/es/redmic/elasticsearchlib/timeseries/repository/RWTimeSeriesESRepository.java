package es.redmic.elasticsearchlib.timeseries.repository;

import org.joda.time.format.DateTimeFormat;

import es.redmic.elasticsearchlib.data.repository.RWDataESRepository;
import es.redmic.models.es.common.model.BaseTimeDataAbstractES;
import es.redmic.models.es.common.query.dto.DataQueryDTO;

public abstract class RWTimeSeriesESRepository<TModel extends BaseTimeDataAbstractES, TQueryDTO extends DataQueryDTO>
		extends RWDataESRepository<TModel, TQueryDTO> implements IBaseTimeSeriesESRepository {

	public RWTimeSeriesESRepository() {
		super(IBaseTimeSeriesESRepository.INDEX, IBaseTimeSeriesESRepository.TYPE, true);
	}

	@Override
	protected String getIndex(final TModel modelToIndex) {
		return getIndex()[0] + "-" + modelToIndex.getDate().toString(DateTimeFormat.forPattern("yyyy-MM-dd"));
	}
}