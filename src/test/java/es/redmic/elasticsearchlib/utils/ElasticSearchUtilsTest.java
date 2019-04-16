package es.redmic.elasticsearchlib.utils;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import es.redmic.elasticsearchlib.common.utils.ElasticSearchUtils;

public class ElasticSearchUtilsTest {

	@Test
	public void should_return_arrayStringWithBoostFieldname_when_callFieldname() throws Exception {

		String[] listIn = new String[] { "title^5" };
		String fieldExpected = "title.suggest^5";

		String[] listOut = ElasticSearchUtils.getSuggestFields(listIn);

		assertTrue(listOut.length == 1);
		assertEquals(listOut[0], fieldExpected);

	}

	@Test
	public void should_return_arrayStringFieldname_when_callFieldname() throws Exception {

		String[] listIn = new String[] { "title" };
		String fieldExpected = "title.suggest";

		String[] listOut = ElasticSearchUtils.getSuggestFields(listIn);

		assertTrue(listOut.length == 1);
		assertEquals(listOut[0], fieldExpected);

	}
}
