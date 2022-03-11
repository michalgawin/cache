package pl.garnizon.memdb;

import java.util.Map;

public interface Snapshot extends Database {

    void merge(Snapshot transaction);

    Map<String, String> getSnapshot();

}
