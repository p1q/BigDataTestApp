package com.prokop.app.data.big.test.bigdatatest.model;

import lombok.Data;

@Data
public class FileIdentifier {
  private String name;
  private String generation;

  public FileIdentifier(String name, String generation) {
    this.name = name;
    this.generation = generation;
  }
}
