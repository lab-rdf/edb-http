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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.jebtk.core.collections.CollectionUtils;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The Class Search.
 */
public class Filter {
  private Filter() {
    // Do nothing
  }

  /**
   * Filter a list of samples to only contain those of a specific type.
   * 
   * @param samples
   * @param types
   * @return
   */
  public static List<SampleBean> filterByTypes(List<SampleBean> samples,
      Collection<Integer> types) {
    if (CollectionUtils.isNullOrEmpty(types)) {
      return samples;
    }

    List<SampleBean> ret = new ArrayList<SampleBean>(samples.size());

    for (SampleBean sample : samples) {
      int type = sample.getType();

      if (types.contains(type)) {
        ret.add(sample);
      }
    }

    return ret;
  }

  public static List<SampleBean> filterByOrganisms(List<SampleBean> samples,
      Collection<Integer> organisms) {
    if (CollectionUtils.isNullOrEmpty(organisms)) {
      return samples;
    }

    List<SampleBean> ret = new ArrayList<SampleBean>(samples.size());

    for (SampleBean sample : samples) {
      int organism = sample.getOrganismId();

      if (organisms.contains(organism)) {
        ret.add(sample);
      }
    }

    return ret;
  }

  public static List<SampleBean> filterByGroups(JdbcTemplate jdbcTemplate,
      AuthBean auth,
      List<SampleBean> samples,
      List<Integer> gids,
      boolean allMode) throws SQLException {
    return filterByGroups(jdbcTemplate,
        auth,
        samples,
        CollectionUtils.toSet(gids),
        allMode);
  }

  /**
   * Filter samples by which groups it belongs to.
   * @param jdbcTemplate
   * @param auth
   * @param samples
   * @param gids          Group ids to search
   * @param allMode       Whether sample must belong to all groups to be 
   *                      returned.
   * @return
   * @throws SQLException 
   */
  public static List<SampleBean> filterByGroups(JdbcTemplate jdbcTemplate,
      AuthBean auth,
      List<SampleBean> samples,
      Collection<Integer> gids,
      boolean allMode) throws SQLException {

    List<SampleBean> ret = new ArrayList<SampleBean>(samples.size());

    if (allMode) {
      // Add sample only if it belongs to all groups

      if (gids.size() == 0) {
        // If there are no group ids specified, then match to all
        gids = Persons.groupIds(jdbcTemplate, auth.getId());
      }

      for (SampleBean sample : samples) {
        boolean include = true;

        Set<Integer> sgids = CollectionUtils.toSet(Samples.getGroups(jdbcTemplate, sample.getId())); //sample.getGroups());

        // Each sample must in all of the groups
        for (int gid : gids) {
          if (!sgids.contains(gid)) {
            include = false;
            break;
          }
        }

        if (include) {
          ret.add(sample);
        }
      }
    } else {
      // Add sample if it belongs to any of the groups

      if (CollectionUtils.isNullOrEmpty(gids)) {
        return samples;
      }

      for (SampleBean sample : samples) {
        for (int gid : Samples.getGroups(jdbcTemplate, sample.getId())) {
          if (gids.contains(gid)) {
            ret.add(sample);
            break;
          }
        }
      }
    }

    return ret;
  }
}
