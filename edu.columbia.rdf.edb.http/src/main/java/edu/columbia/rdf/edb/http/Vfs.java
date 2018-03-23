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

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jebtk.core.io.PathUtils;
import org.jebtk.database.ResultsSetTable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

// TODO: Auto-generated Javadoc
/**
 * The Class Vfs.
 */
public class Vfs {

  /** The Constant VFS_FILE_SQL. */
  public static final String VFS_FILE_SQL = "SELECT vfs.id, vfs.parent_id, vfs.name, vfs.type_id, vfs.path, TO_CHAR(vfs.created, 'YYYY-MM-DD') AS created FROM vfs WHERE vfs.id = ? ORDER BY vfs.id";

  /** The Constant VFS_FILES_SQL. */
  private static final String VFS_FILES_SQL = "SELECT vfs.id, vfs.parent_id, vfs.name, vfs.type_id, vfs.path, TO_CHAR(vfs.created, 'YYYY-MM-DD') AS created FROM vfs WHERE vfs.id = ANY(?::int[]) ORDER BY vfs.id";

  /** The Constant VFS_TAGS_SQL. */
  private static final String VFS_TAGS_SQL = "SELECT vfs_tags.id, vfs_tags.value FROM vfs_tags";

  /** The Constant VFS_TAG_SQL. */
  private static final String VFS_TAG_SQL = VFS_TAGS_SQL
      + " WHERE vfs_tags.vfs_id = ?";

  /** The Constant EXPERIMENT_FILE_IDS_SQL. */
  private static final String EXPERIMENT_FILE_IDS_SQL = "SELECT experiment_files.vfs_id FROM experiment_files WHERE experiment_files.experiment_id = ? ORDER BY experiment_files.vfs_id";

  /** The Constant EXPERIMENT_IDS_SQL. */
  private static final String EXPERIMENT_IDS_SQL = "SELECT experiment_files.experiment_id FROM experiment_files WHERE experiment_files.vfs_id = ?";

  /** The Constant EXPERIMENT_FILE_DIR_SQL. */
  private static final String EXPERIMENT_FILE_DIR_SQL = "SELECT min(experiment_files.vfs_id) FROM experiment_files WHERE experiment_files.experiment_id = ? GROUP BY experiment_files.experiment_id";

  /** The Constant SAMPLE_FILE_IDS_SQL. */
  private static final String SAMPLE_FILE_IDS_SQL = "SELECT sample_files.vfs_id FROM sample_files WHERE sample_files.sample_id = ? ORDER BY sample_files.vfs_id";

  /** The Constant VFS_SAMPLE_IDS_SQL. */
  private static final String VFS_SAMPLE_IDS_SQL = "SELECT sample_files.sample_id FROM sample_files WHERE sample_files.vfs_id = ?";

  /** The Constant SAMPLE_FILE_DIR_SQL. */
  private static final String SAMPLE_FILE_DIR_SQL = "SELECT min(sample_files.vfs_id) FROM sample_files WHERE sample_files.sample_id = ? GROUP BY sample_files.sample_id";

  /** The Constant VFS_SAMPLE_DIR_SQL. */
  private static final String VFS_SAMPLE_DIR_SQL = SAMPLE_FILE_IDS_SQL
      + " LIMIT 1";

  /** The Constant VFS_PATH_SQL. */
  private static final String VFS_PATH_SQL = "SELECT vfs.path FROM vfs WHERE vfs.id = ?";

  /** Convert resultset into bean object. */
  public static final RowMapper<VfsFileBean> VFS_BEAN_MAPPER = new RowMapper<VfsFileBean> () {
    @Override
    public VfsFileBean mapRow(ResultSet rs, int rowNum) throws SQLException {
      return new VfsFileBean(rs.getInt(1), rs.getInt(2), rs.getString(3),
          rs.getInt(4), rs.getString(5), rs.getString(6));
    }
  };


  /**
   * Gets the file table.
   *
   * @param connection the connection
   * @param vfsId the vfs id
   * @return the file table
   * @throws SQLException the SQL exception
   */
  public static ResultsSetTable getFileTable(Connection connection, int vfsId)
      throws SQLException {
    return Database.getTable(connection, VFS_FILE_SQL, vfsId);
  }

  /**
   * Gets the files table.
   *
   * @param connection the connection
   * @param vfsIds the vfs ids
   * @return the files table
   * @throws SQLException the SQL exception
   */
  public static ResultsSetTable getFilesTable(Connection connection,
      final List<Integer> vfsIds) throws SQLException {
    return Database.getTable(connection, VFS_FILES_SQL, vfsIds);
  }

  /**
   * Gets the vfs tags.
   *
   * @param connection the connection
   * @return the vfs tags
   * @throws SQLException the SQL exception
   */
  public static List<Integer> getVfsTags(Connection connection)
      throws SQLException {
    return Database.getIds(connection, VFS_TAGS_SQL);
  }

  /**
   * Gets the vfs tags table.
   *
   * @param connection the connection
   * @return the vfs tags table
   * @throws SQLException the SQL exception
   */
  public static ResultsSetTable getVfsTagsTable(Connection connection)
      throws SQLException {
    return Database.getTable(connection, VFS_TAGS_SQL);
  }

  /**
   * Gets the vfs tags.
   *
   * @param connection the connection
   * @param vfsId the vfs id
   * @return the vfs tags
   * @throws SQLException the SQL exception
   */
  public static List<Integer> getVfsTags(Connection connection, int vfsId)
      throws SQLException {
    return Database.getIds(connection, VFS_TAG_SQL, vfsId);
  }

  /**
   * Gets the vfs tags table.
   *
   * @param connection the connection
   * @param vfsId the vfs id
   * @return the vfs tags table
   * @throws SQLException the SQL exception
   */
  public static List<Integer> getVfsTagsTable(Connection connection, int vfsId)
      throws SQLException {
    return Database.getIds(connection, VFS_TAG_SQL, vfsId);
  }

  /**
   * Gets the experiment files table.
   *
   * @param connection the connection
   * @param experimentId the experiment id
   * @return the experiment files table
   * @throws SQLException the SQL exception
   */
  public static ResultsSetTable getExperimentFilesTable(Connection connection,
      int experimentId) throws SQLException {
    List<Integer> vfsIds = getExperimentFiles(connection, experimentId);

    return getFilesTable(connection, vfsIds);
  }

  /**
   * Gets the experiment files.
   *
   * @param connection the connection
   * @param experimentId the experiment id
   * @return the experiment files
   * @throws SQLException the SQL exception
   */
  public static List<Integer> getExperimentFiles(Connection connection,
      int experimentId) throws SQLException {
    return Database.getIds(connection, EXPERIMENT_FILE_IDS_SQL, experimentId);
  }

  /**
   * Returns the experiments associated with a sample.
   *
   * @param connection the connection
   * @param vfsId the vfs id
   * @return the experiments
   * @throws SQLException the SQL exception
   */
  public static List<Integer> getExperiments(Connection connection, int vfsId)
      throws SQLException {
    return Database.getIds(connection, EXPERIMENT_IDS_SQL, vfsId);
  }

  /**
   * Gets the experiment files dir table.
   *
   * @param connection the connection
   * @param experimentId the experiment id
   * @return the experiment files dir table
   * @throws SQLException the SQL exception
   */
  public static ResultsSetTable getExperimentFilesDirTable(
      Connection connection,
      int experimentId) throws SQLException {
    int vfsId = getExperimentFilesDir(connection, experimentId);

    return getFileTable(connection, vfsId);
  }

  /**
   * Gets the experiment files dir.
   *
   * @param connection the connection
   * @param experimentId the experiment id
   * @return the experiment files dir
   * @throws SQLException the SQL exception
   */
  public static int getExperimentFilesDir(Connection connection,
      int experimentId) throws SQLException {
    return Database.getId(connection, EXPERIMENT_FILE_DIR_SQL, experimentId);
  }

  /**
   * Gets the sample files table.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @return the sample files table
   * @throws SQLException the SQL exception
   */
  public static ResultsSetTable getSampleFilesTable(Connection connection,
      int sampleId) throws SQLException {
    List<Integer> vfsIds = getSampleFiles(connection, sampleId);

    return getFilesTable(connection, vfsIds);
  }

  /**
   * Gets the sample files.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @return the sample files
   * @throws SQLException the SQL exception
   */
  public static List<Integer> getSampleFiles(Connection connection,
      int sampleId) throws SQLException {
    return Database.getIds(connection, SAMPLE_FILE_IDS_SQL, sampleId);
  }

  public static List<Integer> getSampleFiles(JdbcTemplate connection,
      int sampleId) throws SQLException {
    return Query.asIntList(connection, SAMPLE_FILE_IDS_SQL, sampleId);
  }

  /**
   * Returns the samples associated with a vfs id.
   *
   * @param connection the connection
   * @param vfsId the vfs id
   * @return the samples
   * @throws SQLException the SQL exception
   */
  public static List<Integer> getSamples(Connection connection, int vfsId)
      throws SQLException {
    return Database.getIds(connection, VFS_SAMPLE_IDS_SQL, vfsId);
  }

  /**
   * Gets the sample files dir table.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @return the sample files dir table
   * @throws SQLException the SQL exception
   */
  public static ResultsSetTable getSampleFilesDirTable(Connection connection,
      int sampleId) throws SQLException {
    int vfsId = getSampleFilesDir(connection, sampleId);

    return getFileTable(connection, vfsId);
  }

  public static VfsFileBean getSampleFileDir(JdbcTemplate connection,
      int sampleId) throws SQLException {
    int vfsId = getSampleFilesDir(connection, sampleId);

    return getFile(connection, vfsId);
  }

  /**
   * Gets the sample files dir.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @return the sample files dir
   * @throws SQLException the SQL exception
   */
  public static int getSampleFilesDir(Connection connection, int sampleId)
      throws SQLException {
    return Database.getId(connection, SAMPLE_FILE_DIR_SQL, sampleId);
  }

  public static int getSampleFilesDir(JdbcTemplate connection, int sampleId)
      throws SQLException {
    return Database.getId(connection, SAMPLE_FILE_DIR_SQL, sampleId);
  }

  /**
   * Returns the directory path associated with a sample.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @return the sample dir path
   * @throws SQLException the SQL exception
   */
  public static Path getSampleDirPath(Connection connection, int sampleId)
      throws SQLException {
    int vfsId = Database.getId(connection, VFS_SAMPLE_DIR_SQL, sampleId);

    Path path = PathUtils
        .getPath(Database.getString(connection, VFS_PATH_SQL, vfsId));

    return path;
  }

  public static Path getSampleDirPath(JdbcTemplate jdbcTemplate, int sampleId) {
    int vfsId = Database.getId(jdbcTemplate, VFS_SAMPLE_DIR_SQL, sampleId);

    Path path = PathUtils
        .getPath(Database.getString(jdbcTemplate, VFS_PATH_SQL, vfsId));

    return path;
  }

  public static List<VfsFileBean> getFiles(JdbcTemplate connection,
      Collection<Integer> ids) throws SQLException {
    List<VfsFileBean> ret = new ArrayList<VfsFileBean>(1000);

    for (int id : ids) {
      ret.add(getFile(connection, id));
    }

    return ret;
  }

  /**
   * Return a file bean from a given file id.
   * 
   * @param jdbcTemplate
   * @param fid
   * @return
   * @throws SQLException
   */
  public static VfsFileBean getFile(JdbcTemplate jdbcTemplate, int fid)
      throws SQLException {
    //List<VfsFileBean> files = jdbcTemplate.query(Vfs.VFS_FILE_SQL,
    //    new Object[] { fid },
    //    VFS_BEAN_MAPPER);
    
    return Query.query(jdbcTemplate, VFS_FILE_SQL, VFS_BEAN_MAPPER, fid);
  }

}
