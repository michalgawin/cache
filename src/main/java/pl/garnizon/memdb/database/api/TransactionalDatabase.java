package pl.garnizon.memdb.database.api;

public interface TransactionalDatabase<T> extends Database<T>, Transaction {
}
