package pl.garnizon.cache.snapshot.api;

import pl.garnizon.cache.api.Database;

import java.util.Map;

public interface Snapshot<T> extends Database<T> {

    void merge(Snapshot transaction);

    Map<String, T> getSnapshot();

}
