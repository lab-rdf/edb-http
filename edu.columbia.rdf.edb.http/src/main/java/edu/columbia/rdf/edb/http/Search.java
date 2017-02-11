package edu.columbia.rdf.edb.http;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import org.abh.common.collections.CollectionUtils;
import org.abh.common.database.DatabaseResultsTable;
import org.abh.common.search.SearchStackElement;

public class Search {

	private static final String KEYWORD_SQL = 
			"SELECT DISTINCT search_keywords.id FROM search_keywords WHERE search_keywords.name LIKE ?";

	//private static final String TAG_KEYWORD_SQL =
	//		"SELECT DISTINCT search_fields_keywords.id FROM search_fields_keywords WHERE search_fields_keywords.field_id = ? AND search_fields_keywords.keyword_id = ?";

	private static final String ALL_TAG_KEYWORD_SQL =
			"SELECT DISTINCT search_fields_keywords.id FROM search_fields_keywords WHERE search_fields_keywords.field_id = ? AND search_fields_keywords.keyword_id = ANY(?::int[])";

	
	private static final String TAG_KEYWORDS_SQL =
			"SELECT DISTINCT search_fields_keywords.id FROM search_fields_keywords WHERE search_fields_keywords.field_id = ?";

	//private static final String SEARCH_SQL = 
	//		"SELECT DISTINCT search_samples.sample_id FROM search_samples WHERE search_samples.search_field_keyword_id = ?";

	//private static final String SEARCH_LIMIT_SQL = SEARCH_SQL + " LIMIT ?";
//
	
	private static final String ALL_SEARCH_SQL = 
			"SELECT DISTINCT search_samples.sample_id FROM search_samples WHERE search_samples.search_field_keyword_id = ANY(?::int[])";

	//private static final String ALL_SEARCH_LIMIT_SQL = ALL_SEARCH_SQL + " LIMIT ?";


	private Search() {
		// Do nothing
	}

	public static DatabaseResultsTable search(Connection connection, 
			int tagId,
			List<SearchStackElement<Integer>> searchQueue,
			int maxCount) throws SQLException {

		if (searchQueue.size() == 0) {
			//return Database.getSamplesTable(connection);
			
			// Get all the key word ids for the tag
			List<Integer> tagKeywordIds = getTagKeywordIds(connection, tagId);
			
			List<Integer> ids = getSampleIds(connection, tagKeywordIds);
			
			return Database.getSamplesTable(connection, ids, maxCount);
		}
		
		
		//SearchStackElement<Integer> e = null;

		Deque<List<Integer>> tempStack =
				new ArrayDeque<List<Integer>>();

		//while (!searchStack.isEmpty()) {
		//	e = searchStack.pop();
		
		for (SearchStackElement<Integer> e : searchQueue) {

			switch (e.mOp) {
			case MATCH:
				// First get a keyword index
				List<Integer> keywordIds = getKeywordIds(connection, e.mText);

				List<Integer> tagKeywordIds = 
						getTagKeywordIds(connection, tagId, keywordIds);

				tempStack.push(getSampleIds(connection, tagKeywordIds));

				break;
			case AND:
				tempStack.push(CollectionUtils.intersect(tempStack.pop(), 
						tempStack.pop()));
				break;
			case OR:
				tempStack.push(CollectionUtils.union(tempStack.pop(), 
						tempStack.pop()));
				break;
			default:
				break;
			}
		}
		
		// The result will be left on the tempStack
		
		return Database.getSamplesTable(connection, tempStack.pop(), maxCount);
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
	 * @param connection
	 * @param tagKeywordIds
	 * @return
	 * @throws SQLException
	 */
	private static List<Integer> getSampleIds(Connection connection, 
			List<Integer> tagKeywordIds) throws SQLException {
		return Database.getIds(connection, 
				ALL_SEARCH_SQL, 
				tagKeywordIds);
	}

	/*
	private static List<Integer> getTagKeywordIds(Connection connection, 
			int tagId, 
			int keywordId) throws SQLException {
		return Database.getIds(connection, TAG_KEYWORD_SQL, tagId, keywordId);
	}
	*/
	
	private static List<Integer> getTagKeywordIds(Connection connection, 
			int tagId, 
			List<Integer> keywordIds) throws SQLException {
		return Database.getIds(connection, ALL_TAG_KEYWORD_SQL, tagId, keywordIds);
	}
	
	/**
	 * Get all keyword ids associated with a tag.
	 * 
	 * @param connection
	 * @param tagId
	 * @return
	 * @throws SQLException
	 */
	private static List<Integer> getTagKeywordIds(Connection connection, 
			int tagId) throws SQLException {
		return Database.getIds(connection, TAG_KEYWORDS_SQL, tagId);
	}

	public static List<Integer> getKeywordIds(Connection connection, 
			String keyword) throws SQLException {
		return Database.getIds(connection, KEYWORD_SQL, keyword);
	}

}
