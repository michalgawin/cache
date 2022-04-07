package pl.garnizon.cache.api;

public interface Transaction {

    Transaction begin();

    Transaction rollback();

    Transaction commit();

}
