package pl.garnizon.memdb;

public interface Database {

    Database set(String key, String value);

    String get(String key);

    Database delete(String key);

    long count(String value);

}
