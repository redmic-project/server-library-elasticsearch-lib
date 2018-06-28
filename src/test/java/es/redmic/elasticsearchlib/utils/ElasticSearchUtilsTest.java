package es.redmic.elasticsearchlib.utils;

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
