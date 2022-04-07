package pl.garnizon.cache.snapshot.impl;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import pl.garnizon.cache.entity.api.Entity;
import pl.garnizon.cache.entity.impl.EntityImpl;
import pl.garnizon.cache.snapshot.api.AbstractSnapshot;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

public class SnapshotImpl<T extends Comparable<T>> extends AbstractSnapshot<T> {

    public static final Entity<Object> TOMBSTONE = new EntityImpl(null);

    public static <T extends Comparable<T>> SnapshotImpl<T> create() {
        return new SnapshotImpl<>(new HashMap<>(), TreeMultimap.create());
    }

    public SnapshotImpl(Map<String, T> memtable, Multimap<T, String> invertedIndex) {
        super(memtable, invertedIndex);
    }

    @Override
    @Nonnull
    public T getTombstone() {
        return (T) TOMBSTONE;
    }

}
