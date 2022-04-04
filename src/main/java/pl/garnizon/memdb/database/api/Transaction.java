package pl.garnizon.memdb.database.api;

public interface Transaction {

    Transaction begin();

    Transaction rollback();

    Transaction commit();

}
