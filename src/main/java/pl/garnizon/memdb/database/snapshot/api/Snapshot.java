package pl.garnizon.memdb.database.snapshot.api;

import pl.garnizon.memdb.database.api.Database;

import java.util.Map;

public interface Snapshot<T> extends Database<T> {

    void merge(Snapshot transaction);

    Map<String, T> getSnapshot();

}
