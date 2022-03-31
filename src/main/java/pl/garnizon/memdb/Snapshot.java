package pl.garnizon.memdb;

import java.util.Map;

public interface Snapshot<T> extends Database<T> {

    void merge(Snapshot transaction);

    Map<String, T> getSnapshot();

}
