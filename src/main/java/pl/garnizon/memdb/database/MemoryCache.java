package pl.garnizon.memdb.database;

import pl.garnizon.memdb.database.api.Transaction;
import pl.garnizon.memdb.database.api.TransactionalDatabase;
import pl.garnizon.memdb.database.entity.api.Entity;
import pl.garnizon.memdb.database.entity.impl.EntityImpl;
import pl.garnizon.memdb.database.impl.DatabaseImpl;

public class MemoryCache<T extends Comparable<T>> implements TransactionalDatabase<T> {

    TransactionalDatabase<Entity<T>> database;

    public static <T extends Comparable<T>> MemoryCache<T> create() {
        return new MemoryCache<>(DatabaseImpl.create());
    }

    MemoryCache(TransactionalDatabase<Entity<T>> database) {
        this.database = database;
    }

    @Override
    public TransactionalDatabase<T> set(String key, T value) {
        database.set(key, new EntityImpl<>(value));
        return this;
    }

    @Override
    public T get(String key) {
        return database.get(key).getValue();
    }

    @Override
    public TransactionalDatabase<T> delete(String key) {
        database.delete(key);
        return this;
    }

    @Override
    public long count(T value) {
        return database.count(new EntityImpl<>(value));
    }

    @Override
    public TransactionalDatabase<T> begin() {
        database.begin();
        return this;
    }

    @Override
    public Transaction rollback() {
        database.rollback();
        return this;
    }

    @Override
    public Transaction commit() {
        database.commit();
        return this;
    }

}
