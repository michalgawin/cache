package pl.garnizon.memdb;

public interface Transaction {

    Transaction begin();

    Transaction rollback();

    Transaction commit();

}
