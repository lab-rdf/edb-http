package edu.columbia.rdf.edb.http;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.abh.common.database.DatabaseResultsTable;
import org.abh.common.file.PathUtils;

public class Vfs {

	private static final String VFS_FILE_SQL = 
			"SELECT vfs.id, vfs.parent_id, vfs.name, vfs.type_id, vfs.path, TO_CHAR(vfs.created, 'YYYY-MM-DD') AS created FROM vfs WHERE vfs.id = ? ORDER BY vfs.id";
	
	private static final String VFS_FILES_SQL = 
			"SELECT vfs.id, vfs.parent_id, vfs.name, vfs.type_id, vfs.path, TO_CHAR(vfs.created, 'YYYY-MM-DD') AS created FROM vfs WHERE vfs.id = ANY(?::int[]) ORDER BY vfs.id";
	
	private static final String VFS_TAGS_SQL =
			"SELECT vfs_tags.id, vfs_tags.value FROM vfs_tags";
	
	private static final String VFS_TAG_SQL = VFS_TAGS_SQL + " WHERE vfs_tags.vfs_id = ?";

	private static final String EXPERIMENT_FILE_IDS_SQL = 
			"SELECT experiment_files.vfs_id FROM experiment_files WHERE experiment_files.experiment_id = ? ORDER BY experiment_files.vfs_id";

	private static final String EXPERIMENT_IDS_SQL = 
			"SELECT experiment_files.experiment_id FROM experiment_files WHERE experiment_files.vfs_id = ?";

	
	private static final String EXPERIMENT_FILE_DIR_SQL = 
			"SELECT min(experiment_files.vfs_id) FROM experiment_files WHERE experiment_files.experiment_id = ? GROUP BY experiment_files.experiment_id";

	private static final String SAMPLE_FILE_IDS_SQL = 
			"SELECT sample_files.vfs_id FROM sample_files WHERE sample_files.sample_id = ? ORDER BY sample_files.vfs_id";

	private static final String SAMPLE_IDS_SQL = 
			"SELECT sample_files.sample_id FROM sample_files WHERE sample_files.vfs_id = ?";
	
	
	private static final String SAMPLE_FILE_DIR_SQL = 
			"SELECT min(sample_files.vfs_id) FROM sample_files WHERE sample_files.sample_id = ? GROUP BY sample_files.sample_id";
	
	
	private static final String VFS_SAMPLE_DIR_SQL = 
			SAMPLE_FILE_IDS_SQL + " LIMIT 1";
	
	private static final String VFS_PATH_SQL = 
			"SELECT vfs.path FROM vfs WHERE vfs.id = ?";
	
	/*
	public static DatabaseResultsTable getExperimentFilesTable(Connection connection, int sampleId) {
		// TODO Auto-generated method stub
		return null;
	}
	*/

	public static DatabaseResultsTable getFileTable(Connection connection, int vfsId) throws SQLException {
		return Database.getTable(connection, VFS_FILE_SQL, vfsId);
	}
	
	public static DatabaseResultsTable getFilesTable(Connection connection, 
			final List<Integer> vfsIds) throws SQLException {
		return Database.getTable(connection, VFS_FILES_SQL, vfsIds);
	}
	
	public static List<Integer> getVfsTags(Connection connection) throws SQLException {
		return Database.getIds(connection, VFS_TAGS_SQL);
	}
	
	public static DatabaseResultsTable getVfsTagsTable(Connection connection) throws SQLException {
		return Database.getTable(connection, VFS_TAGS_SQL);
	}

	public static List<Integer> getVfsTags(Connection connection, int vfsId) throws SQLException {
		return Database.getIds(connection, VFS_TAG_SQL, vfsId);
	}
	
	public static List<Integer> getVfsTagsTable(Connection connection, int vfsId) throws SQLException {
		return Database.getIds(connection, VFS_TAG_SQL, vfsId);
	}
	
	public static DatabaseResultsTable getExperimentFilesTable(Connection connection,
			int experimentId) throws SQLException {
		List<Integer> vfsIds = getExperimentFiles(connection, experimentId);
		
		return getFilesTable(connection, vfsIds);
	}
	
	public static List<Integer> getExperimentFiles(Connection connection, 
			int experimentId) throws SQLException {
		return Database.getIds(connection, EXPERIMENT_FILE_IDS_SQL, experimentId);
	}
	
	/**
	 * Returns the experiments associated with a sample.
	 * 
	 * @param connection
	 * @param experimentId
	 * @return
	 * @throws SQLException
	 */
	public static List<Integer> getExperiments(Connection connection, 
			int vfsId) throws SQLException {
		return Database.getIds(connection, EXPERIMENT_IDS_SQL, vfsId);
	}
	
	public static DatabaseResultsTable getExperimentFilesDirTable(Connection connection,
			int experimentId) throws SQLException {
		int vfsId = getExperimentFilesDir(connection, experimentId);
		
		return getFileTable(connection, vfsId);
	}
	
	public static int getExperimentFilesDir(Connection connection, 
			int experimentId) throws SQLException {
		return Database.getId(connection, EXPERIMENT_FILE_DIR_SQL, experimentId);
	}

	public static DatabaseResultsTable getSampleFilesTable(Connection connection,
			int sampleId) throws SQLException {
		List<Integer> vfsIds = getSampleFiles(connection, sampleId);
		
		return getFilesTable(connection, vfsIds);
	}
	
	public static List<Integer> getSampleFiles(Connection connection, 
			int sampleId) throws SQLException {
		return Database.getIds(connection, SAMPLE_FILE_IDS_SQL, sampleId);
	}
	
	/**
	 * Returns the samples associated with a vfs id.
	 * 
	 * @param connection
	 * @param vfsId
	 * @return
	 * @throws SQLException
	 */
	public static List<Integer> getSamples(Connection connection, 
			int vfsId) throws SQLException {
		return Database.getIds(connection, SAMPLE_IDS_SQL, vfsId);
	}
	
	public static DatabaseResultsTable getSampleFilesDirTable(Connection connection,
			int sampleId) throws SQLException {
		int vfsId = getSampleFilesDir(connection, sampleId);
		
		return getFileTable(connection, vfsId);
	}
	
	public static int getSampleFilesDir(Connection connection, 
			int sampleId) throws SQLException {
		return Database.getId(connection, SAMPLE_FILE_DIR_SQL, sampleId);
	}

	
	
	/**
	 * Returns the directory path associated with a sample.
	 * 
	 * @param connection
	 * @param sampleId
	 * @return
	 * @throws SQLException
	 */
	public static Path getSampleDirPath(Connection connection, int sampleId) throws SQLException {
		int vfsId = Database.getId(connection, VFS_SAMPLE_DIR_SQL, sampleId);
		
		Path path = PathUtils.getPath(Database.getString(connection, VFS_PATH_SQL, vfsId));
		
		return path;
	}

	

	/*
	public static List<Integer> getVfsTags(Connection connection, int vfsId) {
		List<Integer> ret = Collections.emptyList();
		
		PreparedStatement statement = 
				connection.prepareStatement(VFS_SQL);

		DatabaseResultsTable table = null;

		try {
			statement.setInt(1, vfsId);

			table = JDBCConnection.getTable(statement);
		} finally {
			statement.close();
		}
	}
	*/

}
