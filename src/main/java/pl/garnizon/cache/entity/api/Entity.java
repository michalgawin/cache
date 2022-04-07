package pl.garnizon.cache.entity.api;

public interface Entity<T> extends Comparable<Entity<T>> {

    T getValue();

}
