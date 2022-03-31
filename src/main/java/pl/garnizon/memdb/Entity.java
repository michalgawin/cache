package pl.garnizon.memdb;

public interface Entity<T> extends Comparable<Entity<T>> {

    T getValue();

}
