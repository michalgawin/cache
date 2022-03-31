package pl.garnizon.memdb;

public class DatabaseSystem<T extends Comparable<T>> implements TransactionalDatabase<T> {

    TransactionalDatabase<Entity<T>> database;

    public static <T extends Comparable<T>> DatabaseSystem<T> create() {
        return new DatabaseSystem<>(DatabaseImpl.create());
    }

    DatabaseSystem(TransactionalDatabase<Entity<T>> database) {
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
