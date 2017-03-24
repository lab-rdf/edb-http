/**
 * Copyright 2017 Antony Holmes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.columbia.rdf.edb.http;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Set;

import org.abh.common.collections.CollectionUtils;
import org.abh.common.database.JDBCConnection;
import org.abh.common.database.ResultsSetTable;
import org.abh.common.search.SearchStackElement;

// TODO: Auto-generated Javadoc
/**
 * The Class Search.
 */
public class Search {

	/** The Constant KEYWORD_SQL. */
	private static final String KEYWORD_SQL = 
			"SELECT DISTINCT keywords.id FROM keywords WHERE keywords.name LIKE ?";

	/** The Constant KEYWORD_NOT_SQL. */
	private static final String KEYWORD_NOT_SQL = 
			"SELECT DISTINCT keywords.id FROM keywords WHERE keywords.name NOT LIKE ?";

	//private static final String TAG_KEYWORD_SQL =
	//		"SELECT DISTINCT search_fields_keywords.id FROM search_fields_keywords WHERE search_fields_keywords.field_id = ? AND search_fields_keywords.keyword_id = ?";



	/** The Constant TAG_KEYWORDS_SQL. */
	private static final String ALL_TAG_KEYWORD_ID_SQL =
			"SELECT DISTINCT tags_keywords_search.id FROM tags_keywords_search WHERE tags_keywords_search.tag_id = ?";

	/** The Constant ALL_TAG_KEYWORD_SQL. */
	private static final String TAG_KEYWORD_ID_SQL =
			ALL_TAG_KEYWORD_ID_SQL + " AND tags_keywords_search.keyword_id = ANY(?::int[])";

	//private static final String SEARCH_SQL = 
	//		"SELECT DISTINCT search_samples.sample_id FROM search_samples WHERE search_samples.search_field_keyword_id = ?";

	//private static final String SEARCH_LIMIT_SQL = SEARCH_SQL + " LIMIT ?";
	//

	/** The Constant ALL_SEARCH_SQL. */
	private static final String ALL_SEARCH_SQL = 
			"SELECT DISTINCT tags_samples_search.sample_id FROM tags_samples_search WHERE tags_samples_search.tag_keyword_search_id = ANY(?::int[])";

	//private static final String ALL_SEARCH_LIMIT_SQL = ALL_SEARCH_SQL + " LIMIT ?";


	/** The Constant ALL_TAG_SAMPLES_SQL. */
	private static final String ALL_TAG_SAMPLES_SQL =
			"SELECT DISTINCT tags_samples_search.sample_id FROM tags_samples_search, tags_keywords_search, keywords WHERE tags_samples_search.tag_keyword_search_id = tags_keywords_search.id AND tags_keywords_search.keyword_id = keywords.id AND tags_keywords_search.tag_id = ?";

	/** The Constant SAMPLE_KEYWORD_SEARCH_CORE_SQL. */
	private static final String SAMPLE_KEYWORD_SEARCH_CORE_SQL =
			ALL_TAG_SAMPLES_SQL + " AND keywords.name ";

	/** The Constant SAMPLE_KEYWORD_SEARCH_SQL. */
	private static final String SAMPLE_KEYWORD_SEARCH_SQL = SAMPLE_KEYWORD_SEARCH_CORE_SQL + "LIKE ?";

	/** The Constant SAMPLE_KEYWORD_SEARCH_NOT_SQL. */
	//private static final String SAMPLE_KEYWORD_SEARCH_NOT_SQL = SAMPLE_KEYWORD_SEARCH_CORE_SQL + "NOT LIKE ?";

	/** The Constant SAMPLE_KEYWORD_EXACT_SEARCH_CORE_SQL. */
	private static final String SAMPLE_KEYWORD_EXACT_SEARCH_CORE_SQL =
			ALL_TAG_SAMPLES_SQL + " AND LOWER(keywords.name) ";

	/** The Constant SAMPLE_KEYWORD_EXACT_SEARCH_SQL. */
	private static final String SAMPLE_KEYWORD_EXACT_SEARCH_SQL = SAMPLE_KEYWORD_EXACT_SEARCH_CORE_SQL + "= ?";

	/** The Constant SAMPLE_KEYWORD_EXACT_SEARCH_NOT_SQL. */
	//private static final String SAMPLE_KEYWORD_EXACT_SEARCH_NOT_SQL = SAMPLE_KEYWORD_EXACT_SEARCH_CORE_SQL + "!= ?";




	/** The Constant SAMPLE_EXACT_SEARCH_CORE_SQL. */
	//private static final String SAMPLE_EXACT_SEARCH_CORE_SQL =
	//		"SELECT tags_sample.sample_id FROM tags_sample WHERE tags_sample.tag_id = ? AND tags_sample.value";

	/** The Constant SAMPLE_EXACT_SEARCH_SQL. */
	//private static final String SAMPLE_EXACT_SEARCH_SQL =
	//		SAMPLE_EXACT_SEARCH_CORE_SQL + " ILIKE ?";

	/** The Constant SAMPLE_EXACT_SEARCH_NOT_SQL. */
	//private static final String SAMPLE_EXACT_SEARCH_NOT_SQL =
	//		SAMPLE_EXACT_SEARCH_CORE_SQL + " NOT ILIKE ?";


	/**
	 * Instantiates a new search.
	 */
	private Search() {
		// Do nothing
	}

	/**
	 * Search.
	 *
	 * @param connection the connection
	 * @param tagId the tag id
	 * @param searchQueue the search queue
	 * @param maxCount the max count
	 * @return the results set table
	 * @throws SQLException the SQL exception
	 */
	public static ResultsSetTable search(Connection connection, 
			int tagId,
			List<SearchStackElement<Integer>> searchQueue,
			int maxCount) throws SQLException {

		if (searchQueue.size() == 0) {
			//return Database.getSamplesTable(connection);

			// Get all the key word ids for the tag
			//List<Integer> tagKeywordIds = getTagKeywordIds(connection, tagId);

			List<Integer> ids = getSampleIds(connection, tagId); //getSampleIds(connection, tagKeywordIds);

			return Database.getSamplesTable(connection, ids, maxCount);
		}


		//SearchStackElement<Integer> e = null;

		Deque<SearchResults> tempStack =
				new ArrayDeque<SearchResults>();

		//while (!searchStack.isEmpty()) {
		//	e = searchStack.pop();

		for (SearchStackElement<Integer> e : searchQueue) {
			switch (e.mOp) {
			case MATCH:
				// First get a list of keywords matching the search
				//List<Integer> keywordIds = getKeywordIds(connection, e.mText);

				// Now see if the tag of interest contains those keyword ids
				//List<Integer> tagKeywordIds = 
				//		getTagKeywordIds(connection, tagId, keywordIds);

				String keyword = e.mText;

				boolean include = include(keyword);

				// Get all samples matched to those keywords for the tag.
				tempStack.push(new SearchResults(getSampleIds(connection, tagId, e.mText, include), include)); //tagKeywordIds); //getSampleIds(connection, tagKeywordIds));

				break;
			case AND:
				tempStack.push(and(tempStack.pop(), tempStack.pop()));
				break;
			case OR:
				tempStack.push(or(tempStack.pop(), tempStack.pop()));
				break;
			default:
				break;
			}
		}

		// The result will be left on the tempStack

		//List<Integer> sampleIds = getSampleIds(connection, tempStack.pop());

		SearchResults results = tempStack.pop();

		Collection<Integer> samples;

		if (results.getInclude()) {
			samples = results.getValues();
		} else {
			samples = Collections.emptySet();
		}

		return Database.getSamplesTable(connection, samples, maxCount); //Database.getSamplesTable(connection, sampleIds, maxCount);
	}

	/**
	 * Performs an intersection of two sets of samples. If either of the
	 * results sets are non-inclusive (find samples without word) we return
	 * the compliment of the inclusive set to the non-inclusive, i.e.
	 * exclude whatever is in the non-inclusive.
	 * 
	 *
	 * @param sr1 		Results 1.
	 * @param sr2 		Results 2.
	 * @return 			The intersection of results 1 and results 2.
	 */
	private static SearchResults and(SearchResults sr1, SearchResults sr2) {

		if (!sr1.getInclude() && sr2.getInclude()) {
			return new SearchResults(CollectionUtils.notIn(sr2.getValues(), sr1.getValues()));
		} else if (sr1.getInclude() && !sr2.getInclude()) {
			return new SearchResults(CollectionUtils.notIn(sr1.getValues(), sr2.getValues()));
		} else {
			// Normal
			return new SearchResults(CollectionUtils.intersect(sr1.getValues(), sr2.getValues()));
		}
	}

	/**
	 * Perform a union operation on two sets of samples. If either of the
	 * results sets are non-inclusive (find samples without word) we simply
	 * return the other set.
	 *
	 * @param sr1 	the sr 1
	 * @param sr2 	the sr 2
	 * @return 		The union of two results.
	 */
	private static SearchResults or(SearchResults sr1, SearchResults sr2) {

		if (!sr1.getInclude() && sr2.getInclude()) {
			return sr2;
		} else if (sr1.getInclude() && !sr2.getInclude()) {
			return sr1;
		} else {
			// Normal
			return new SearchResults(CollectionUtils.union(sr1.getValues(), sr2.getValues()));
		}
	}

	/*
	private static List<Integer> getSampleIds(Connection connection, 
			int tagKeywordId,
			int maxCount) throws SQLException {
		if (maxCount != -1) {
			return Database.getIds(connection, SEARCH_LIMIT_SQL, tagKeywordId, maxCount);
		} else {
			return Database.getIds(connection, SEARCH_SQL, tagKeywordId);
		}
	}
	 */

	/**
	 * Return the samples associated with search keyword ids.
	 *
	 * @param connection the connection
	 * @param tagKeywordIds the tag keyword ids
	 * @return the sample ids
	 * @throws SQLException the SQL exception
	 */
	private static List<Integer> getSampleIds(Connection connection, 
			List<Integer> tagKeywordIds) throws SQLException {
		return Database.getIds(connection, 
				ALL_SEARCH_SQL, 
				tagKeywordIds);
	}

	/**
	 * Return all of the samples associated with a given tag.
	 *
	 * @param connection the connection
	 * @param tagId the tag id
	 * @return the sample ids
	 * @throws SQLException the SQL exception
	 */
	private static List<Integer> getSampleIds(Connection connection, 
			int tagId) throws SQLException {

		return Database.getIds(connection, 
				ALL_TAG_SAMPLES_SQL, 
				tagId);
	}

	/**
	 * Find all the samples matching keywords associated with a particular
	 * tag. Words beginning with '-' will find samples not containg the
	 * word or prefix (similar to a google search).
	 *
	 * @param connection the connection
	 * @param tagId the tag id
	 * @param keyword the keyword
	 * @param include the include
	 * @return the sample ids
	 * @throws SQLException the SQL exception
	 */
	private static Set<Integer> getSampleIds(Connection connection, 
			int tagId,
			String keyword,
			boolean include) throws SQLException {

		boolean exact = exact(keyword);

		if (exact) {
			// Strip quotation marks
			keyword = keyword.substring(1, keyword.length() - 1);
		}

		if (!include) {
			// Strip dash at beginning
			keyword = keyword.substring(1);
		}

		//System.err.println("keyword3 " + keyword + " " + exact + " " + include);

		if (exact) {
			// Match on the tag value for the sample. This is not quite
			// exact but is designed for cases where a word in the value
			// should be included or excluded.

			//System.err.println("keyword4 " + SAMPLE_KEYWORD_EXACT_SEARCH_SQL + " " + tagId + " " + keyword);

			return getIds(connection, 
					SAMPLE_KEYWORD_EXACT_SEARCH_SQL, 
					tagId,
					keyword);
		} else {
			// In the non-exact match we match on indexed keywords. Note
			// that this can make excluding samples difficult since if they
			// are indexed on multiple keywords, -keyword, will eliminate
			// matches based on the keyword, but keep all others making this
			// function effectively useless
			
			//System.err.println("keyword5 " + SAMPLE_KEYWORD_SEARCH_SQL + " " + tagId + " " + keyword);

			return getIds(connection, 
					SAMPLE_KEYWORD_SEARCH_SQL, 
					tagId,
					keyword);
		}
	}

	/**
	 * Return the ids from a query where the first parameter is an integer
	 * (e.g. a tag id) and the second is a string (e.g. a keyword).
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param id1 the id 1
	 * @param id2 the id 2
	 * @return the ids
	 * @throws SQLException the SQL exception
	 */
	public static Set<Integer> getIds(Connection connection, 
			final String sql,
			int id1,
			String id2) throws SQLException {
		Set<Integer> ret = null;

		PreparedStatement statement = 
				connection.prepareStatement(sql);

		try {
			statement.setInt(1, id1);
			statement.setString(2, id2 + "%");

			ret = JDBCConnection.getIntSet(statement);
		} finally {
			statement.close();
		}

		if (ret == null) {
			ret = Collections.emptySet();
		}

		return ret;
	}

	/*
	private static List<Integer> getTagKeywordIds(Connection connection, 
			int tagId, 
			int keywordId) throws SQLException {
		return Database.getIds(connection, TAG_KEYWORD_SQL, tagId, keywordId);
	}
	 */

	/**
	 * Gets the tag keyword ids.
	 *
	 * @param connection the connection
	 * @param tagId the tag id
	 * @param keywordIds the keyword ids
	 * @return the tag keyword ids
	 * @throws SQLException the SQL exception
	 */
	private static List<Integer> getTagKeywordIds(Connection connection, 
			int tagId, 
			List<Integer> keywordIds) throws SQLException {
		return Database.getIds(connection, TAG_KEYWORD_ID_SQL, tagId, keywordIds);
	}

	/**
	 * Get all keyword ids associated with a tag.
	 *
	 * @param connection the connection
	 * @param tagId the tag id
	 * @return the tag keyword ids
	 * @throws SQLException the SQL exception
	 */
	private static List<Integer> getTagKeywordIds(Connection connection, 
			int tagId) throws SQLException {
		return Database.getIds(connection, ALL_TAG_KEYWORD_ID_SQL, tagId);
	}

	/**
	 * Gets the keyword ids.
	 *
	 * @param connection the connection
	 * @param keyword the keyword
	 * @return the keyword ids
	 * @throws SQLException the SQL exception
	 */
	public static List<Integer> getKeywordIds(Connection connection, 
			String keyword) throws SQLException {
		return getKeywordIds(connection, keyword, true);
	}

	/**
	 * Gets the keyword ids.
	 *
	 * @param connection the connection
	 * @param keyword the keyword
	 * @param include the include
	 * @return the keyword ids
	 * @throws SQLException the SQL exception
	 */
	public static List<Integer> getKeywordIds(Connection connection, 
			String keyword,
			boolean include) throws SQLException {
		return Database.getIds(connection, include ? KEYWORD_SQL : KEYWORD_NOT_SQL, keyword);
	}

	/**
	 * Exact.
	 *
	 * @param keyword the keyword
	 * @return true, if successful
	 */
	private static boolean exact(String keyword) {
		return keyword.charAt(0) == '"';
	}

	/**
	 * Include.
	 *
	 * @param keyword the keyword
	 * @return true, if successful
	 */
	private static boolean include(String keyword) {
		if (exact(keyword)) {
			return keyword.charAt(1) != '-';
		} else {
			return keyword.charAt(0) != '-';
		}
	}
}
