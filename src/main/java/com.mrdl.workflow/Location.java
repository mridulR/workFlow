package com.mrdl.workflow;

public class Location {

  private String bucket;
  private String project;
  private String file;

  public Location(Builder builder) {
    this.project = builder.project;
    this.bucket = builder.bucket;
    this.file = builder.file;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getBucket() {
    return bucket;
  }

  public String getProject() {
    return project;
  }

  public String getFile() {
    return file;
  }

  public static final class Builder {
    private String project;
    private String bucket;
    private String file;

    public Builder() {}

    public Builder withProject(String project) {
      this.project = project;
      return this;
    }

    public Builder withBucket(String bucket) {
      this.bucket = bucket;
      return this;
    }

    public Builder withFile(String file) {
      this.file = file;
      return this;
    }

    public Location build() {
      return new Location(this);
    }
  }
}
