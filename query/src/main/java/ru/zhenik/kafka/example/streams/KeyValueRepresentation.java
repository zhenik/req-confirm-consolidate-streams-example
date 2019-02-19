package ru.zhenik.kafka.example.streams;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class KeyValueRepresentation {
  private String key;
  private String value;

  public KeyValueRepresentation() { }

  public KeyValueRepresentation(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override public String toString() {
    return "KeyValueRepresentation{" +
        "key='" + key + '\'' +
        ", value='" + value + '\'' +
        '}';
  }
}
