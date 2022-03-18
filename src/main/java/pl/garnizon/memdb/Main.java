package pl.garnizon.memdb;

public class Main {

    public static void main(String[] args) {
        TransactionalDatabase<String> transactionalDatabase = DatabaseImpl.create();
        transactionalDatabase.set("name", "michal");
        transactionalDatabase.get("name");
        transactionalDatabase.delete("name");
        transactionalDatabase.get("name");
        transactionalDatabase.set("name", "michal");
        transactionalDatabase.set("name2", "michal");
        transactionalDatabase.count("michal");
        transactionalDatabase.begin();
        transactionalDatabase.set("name", "jan");
        transactionalDatabase.begin();
        transactionalDatabase.set("name2", "janek");
        transactionalDatabase.set("name3", "michal");
        transactionalDatabase.rollback();
        transactionalDatabase.set("name4", "michal");
        transactionalDatabase.commit();
        transactionalDatabase.begin();
        transactionalDatabase.delete("name4");
        transactionalDatabase.begin();
        transactionalDatabase.delete("name2");
        transactionalDatabase.commit();
        transactionalDatabase.commit();
        transactionalDatabase.get("name");
        transactionalDatabase.get("name2");

        transactionalDatabase.count("michal");
        transactionalDatabase.commit();
        transactionalDatabase.rollback();
    }

}
