package edu.columbia.rdf.edb.http;

import org.jebtk.core.IdProperty;
import org.jebtk.core.NameGetter;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "id", "pid", "n", "t", "d" })
public class VfsFileBean implements IdProperty, NameGetter {

  private int mId;
  private int mPid;
  private String mName;
  private int mType;
  private String mDate;
  private String mPath;

  public VfsFileBean(int id, int pid, String name, int type, String path,
      String date) {
    mId = id;
    mPid = pid;
    mName = name;
    mType = type;
    mPath = path;
    mDate = date;
  }

  @Override
  public int getId() {
    return mId;
  }

  /**
   * Returns the parent id of the file.
   * 
   * @return
   */
  public int getPid() {
    return mPid;
  }

  /**
   * Returns the name of the file.
   */
  @Override
  @JsonGetter("n")
  public String getName() {
    return mName;
  }

  /**
   * Returns the file type, either a directory (1) or a file (2)
   * 
   * @return
   */
  @JsonGetter("t")
  public int getType() {
    return mType;
  }

  @JsonIgnore
  @JsonGetter("path")
  public String getPath() {
    return mPath;
  }

  /**
   * Returns the file data, typically when it was created or updated.
   * 
   * @return
   */
  @JsonGetter("d")
  public String getDate() {
    return mDate;
  }
}
