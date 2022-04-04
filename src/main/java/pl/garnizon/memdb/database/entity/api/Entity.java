package pl.garnizon.memdb.database.entity.api;

public interface Entity<T> extends Comparable<Entity<T>> {

    T getValue();

}
