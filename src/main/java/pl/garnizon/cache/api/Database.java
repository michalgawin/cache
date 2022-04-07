package pl.garnizon.cache.api;

public interface Database<T> {

    Database<T> set(String key, T value);

    T get(String key);

    Database<T> delete(String key);

    long count(T value);

}
