package pl.garnizon.cache.api;

public interface TransactionalDatabase<T> extends Database<T>, Transaction {
}
