package es.redmic.elasticsearchlib.common.utils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import es.redmic.exception.elasticsearch.ESParseException;
import es.redmic.models.es.common.query.dto.AggsPropertiesDTO;
import es.redmic.models.es.common.query.dto.SortDTO;

@Component
public class ElasticSearchUtils {

	protected final static String SCRIPT_ENGINE = "groovy";
	protected final static String SUGGESTSUFFIX = ".suggest";

	protected final static String PRETAGS = "<b>";
	protected final static String POSTTAGS = "</b>";

	@Autowired
	ObjectMapper jacksonMapper;

	// mapper estático para usarlos en las funciones estáticas de utils
	static ObjectMapper jMapper;

	@PostConstruct
	public void initStaticMapper() {
		jMapper = jacksonMapper;
	}

	/*
	 * Obtiene los nombres de los campos de una clase
	 */

	public static List<String> getFieldsName(Class<?> resultClass) {

		List<String> fieldsName = new ArrayList<String>();

		while (resultClass != null && resultClass.getDeclaredFields() != null) {
			Field[] fields = resultClass.getDeclaredFields();
			for (int j = 0; j < fields.length; j++)
				fieldsName.add(fields[j].getName());
			resultClass = resultClass.getSuperclass();
		}
		return fieldsName;
	}

	public static List<?> parseMGetHit(MultiGetResponse multiResponse, JavaType resultClass) {

		MultiGetItemResponse[] responseList = multiResponse.getResponses();
		List<Object> result = new ArrayList<Object>();
		int responseSize = responseList.length;
		for (int i = 0; i < responseSize; i++) {
			MultiGetItemResponse item = responseList[i];
			GetResponse response = item.getResponse();
			if (response != null && response.isExists()) {
				result.add(jMapper.convertValue(getResponsetoObject(response), resultClass));
			}
		}
		return result;
	}

	/**
	 * Retorna solamente los campos de highlight
	 * 
	 * @param response
	 *            un SearchResponse que contenga hightlight
	 * @return lista de sugerencias
	 */
	public static List<String> createHighlightResponse(SearchResponse response) {
		List<String> highlightResponse = new ArrayList<String>();
		SearchHits hits = response.getHits();
		for (SearchHit hit : hits.getHits()) {
			Map<String, HighlightField> highlightFields = hit.getHighlightFields();
			for (String fieldName : highlightFields.keySet()) {
				HighlightField highlightField = highlightFields.get(fieldName);
				String highlight = highlightField.getFragments()[0].toString();
				if (highlightResponse.indexOf(highlight) < 0)
					highlightResponse.add(highlightField.getFragments()[0].toString());
			}
		}
		return highlightResponse;
	}

	/*
	 * Pasa la respuesta map
	 */
	public static Map<String, Object> getResponsetoObject(GetResponse response) {

		Map<String, Object> result = new HashMap<String, Object>();

		result.put("_index", response.getIndex());
		result.put("_type", response.getType());
		result.put("_id", response.getId());
		result.put("_score", null);
		result.put("_version", response.getVersion());
		result.put("highlight", null);
		result.put("_source", response.getSourceAsMap());

		return result;
	}

	/*
	 * Pasa la respuesta map
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> searchResponsetoObject(SearchResponse response) {

		XContentBuilder builder;
		try {
			builder = XContentFactory.jsonBuilder();
			builder.startObject();
			response.innerToXContent(builder, ToXContentObject.EMPTY_PARAMS);
			builder.endObject();

			return jMapper.readValue(Strings.toString(builder), Map.class);

		} catch (IOException e) {
			throw new ESParseException(e);
		}
	}

	public static QueryBuilder getPostFilter(Map<String, String[]> postFilterObject) {

		// Añade facets si los tiene
		if (postFilterObject != null)
			return postFilterDtoToFilterBuilder(postFilterObject);
		return null;
	}

	public static QueryBuilder postFilterDtoToFilterBuilder(Map<String, String[]> postFilterMap) {

		BoolQueryBuilder base = QueryBuilders.boolQuery();

		for (Entry<String, String[]> entry : postFilterMap.entrySet())
			if (!entry.getKey().contains("$")) {
				base.must(QueryBuilders.termsQuery(entry.getKey(), entry.getValue()));
			} else {
				String param = entry.getKey().split("\\$")[0];
				String key = entry.getKey().replaceAll("\\$", "");

				base.must(QueryBuilders.nestedQuery(param,
						QueryBuilders.boolQuery().should(QueryBuilders.termsQuery(key, entry.getValue())),
						ScoreMode.Avg));
			}
		return base;
	}

	public static List<BaseAggregationBuilder> getAggs(List<AggsPropertiesDTO> aggs) {

		// Añade los aggs si los tiene
		if (aggs != null) {
			List<BaseAggregationBuilder> terms = new ArrayList<BaseAggregationBuilder>();
			for (AggsPropertiesDTO item : aggs) {
				if (item.getField() != null) {

					TermsAggregationBuilder term = AggregationBuilders.terms(item.getTerm()).field(item.getField());

					if (item.getMinCount() != 1)
						term.minDocCount(item.getMinCount());

					term.order(BucketOrder.key(true));

					if (item.getSize() != null)
						term.size(item.getSize());

					if (item.getNested() != null) {
						terms.add(AggregationBuilders.nested(item.getTerm(), item.getNested()).subAggregation(term));
					} else {
						terms.add(term);
					}
				}
			}
			return terms;
		}
		return null;
	}

	public static List<SortBuilder<?>> getSorts(List<SortDTO> sorts) {

		List<SortBuilder<?>> sortBuilders = new ArrayList<SortBuilder<?>>();

		if (sorts == null || sorts.size() == 0)
			return null;

		for (int i = 0; i < sorts.size(); i++)
			sortBuilders.add(SortBuilders.fieldSort(sorts.get(i).getField())
					.order(SortOrder.valueOf(sorts.get(i).getOrder().toUpperCase())));

		return sortBuilders;
	}

	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> getBucketsFromAggregations(Map<String, Object> source) {
		for (Map.Entry<String, Object> entry : source.entrySet()) {
			if (entry.getValue() != null && entry.getValue() instanceof List)
				return (List<Map<String, Object>>) entry.getValue();
			if (entry.getValue() != null && entry.getValue() instanceof Map)
				return getBucketsFromAggregations((Map<String, Object>) entry.getValue());
		}
		return null;
	}

	// TODO: realizar esta acción dependiendo del tipo de modelo. Solo añadir si
	// es necesario
	public static String[] getReturnFields(List<String> fields) {

		if (!fields.contains("id"))
			fields.add("id");

		if (!fields.contains("uuid"))
			fields.add("uuid");

		if (!fields.contains("path"))
			fields.add("path");

		return fields.toArray(new String[fields.size()]);
	}

	/**
	 * Añade a un listado de campos el sufijo .suggest que son los campos en
	 * ElasticSearch donde se han mapeado las sugerencias
	 * 
	 * @param fields
	 *            Listado de campos donde se van a buscar sugerencias
	 * @return Listado de campos modificados con sufijo suggest
	 */
	public static String[] getSuggestFields(String[] fields) {

		String[] suggestFields = new String[fields.length];
		for (int i = 0; i < fields.length; i++) {
			String[] fieldname = fields[i].split("\\^");
			if (fieldname.length == 2)
				suggestFields[i] = fieldname[0] + SUGGESTSUFFIX + "^" + fieldname[1];
			else
				suggestFields[i] = fieldname[0] + SUGGESTSUFFIX;
		}

		return suggestFields;
	}

	public static HighlightBuilder getHighlightBuilder(String[] highlightFields) {

		HighlightBuilder highlightBuilder = new HighlightBuilder();

		for (String field : highlightFields) {
			highlightBuilder.field(field.contains(SUGGESTSUFFIX) ? field : (field + SUGGESTSUFFIX));
		}

		highlightBuilder.preTags(PRETAGS);
		highlightBuilder.postTags(POSTTAGS);

		return highlightBuilder;
	}
}
