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

/**
 * The Enum UserType.
 */
public enum UserType {
  /** The normal. */
  NORMAL,

  /** The administrator. */
  ADMINISTRATOR,

  /** The superuser. */
  SUPERUSER;
  


  /**
   * Gets the from id.
   *
   * @param userTypeId the user type id
   * @return the from id
   */
  public static UserType getFromId(int userTypeId) {
    switch (userTypeId) {
    case 4:
      return SUPERUSER;
    case 3:
      return ADMINISTRATOR;
    default:
      return NORMAL;
    }
  }
  
  /**
   * Returns true if the rank of t2 is at least that of t1.
   * 
   * @param t1
   * @param t2
   * @return
   */
  public static boolean geRank(UserType t1, UserType t2) {
    return t2.compareTo(t1) >= 0;
  }
}
