package com.yugabyte.sample.apps;

import com.fasterxml.jackson.core.JsonParser;
import java.io.IOException;
import java.util.Properties;

public class Config {
  private static final Properties defaults = new Properties();
  static{
    try {
      defaults.load(Config.class.getClassLoader().getResourceAsStream("config.properties"));
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
  public Config(){
    this(defaults);
  }
  String selectQuery;

  public Config(Properties properties) {
    init(properties);
  }
  public void init(Properties init){
    Properties config = new Properties(defaults);
    if ( null != init &&  defaults != init){
      config.putAll(init);
    }
    JsonParser parser;

  }
}
