package es.redmic.elasticsearchlib.common;

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

import org.apache.commons.io.IOUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import es.redmic.elasticsearchlib.common.query.SimpleQueryUtils;
import es.redmic.models.es.common.query.dto.MetadataQueryDTO;
import es.redmic.models.es.common.query.dto.RegexpDTO;
import es.redmic.models.es.common.query.dto.TextQueryDTO;

public class SimpleQueryTest {

	private MetadataQueryDTO queryDTO;

	@Before
	public void setupTest() {

		queryDTO = new MetadataQueryDTO();
	}

	@Test
	public void getQuery_ReturnRegexpQuery_IfQueryDTOHasRegexpDTO() throws IOException, JSONException {

		createRegexpQuery();

		BoolQueryBuilder query = SimpleQueryUtils.getQuery(queryDTO, null, null);

		String queryExpected = getExpectedQuery("/queryfactory/common/regexpQuery.json");

		JSONAssert.assertEquals(queryExpected, query.toString(), false);
	}

	@Test
	public void getQuery_ReturnTextQuery_IfQueryDTOHasTextQueryDTO() throws IOException, JSONException {

		createTextQuery();

		BoolQueryBuilder query = SimpleQueryUtils.getQuery(queryDTO, null, null);

		String queryExpected = getExpectedQuery("/queryfactory/common/textQuery.json");

		JSONAssert.assertEquals(queryExpected, query.toString(), false);
	}

	private String getExpectedQuery(String resourcePath) throws IOException {

		return IOUtils.toString(getClass().getResource(resourcePath).openStream());
	}

	private void createRegexpQuery() {

		List<RegexpDTO> regexpList = new ArrayList<RegexpDTO>();
		RegexpDTO regexp = new RegexpDTO();
		regexp.setField("path");
		regexp.setExp("root.[0-9]+");
		regexpList.add(regexp);
		queryDTO.setRegexp(regexpList);
	}

	private void createTextQuery() {

		TextQueryDTO text = new TextQueryDTO();
		text.setText("prueba");
		text.setSearchFields(new String[] { "name" });
		queryDTO.setText(text);
	}
}
