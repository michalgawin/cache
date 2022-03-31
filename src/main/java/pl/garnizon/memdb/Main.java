package pl.garnizon.memdb;

public class Main {

    public static void main(String[] args) {
        TransactionalDatabase<String> database = DatabaseSystem.create(); //DatabaseImpl.create();
        database.set("name", "michal");
        database.get("name");
        database.delete("name");
        database.get("name");
        database.set("name", "michal");
        database.set("name2", "michal");
        database.count("michal");
        database.begin();
        database.set("name", "jan");
        database.begin();
        database.set("name2", "janek");
        database.set("name3", "michal");
        database.rollback();
        database.set("name4", "michal");
        database.commit();
        database.begin();
        database.delete("name4");
        database.begin();
        database.delete("name2");
        database.commit();
        database.commit();
        database.get("name");
        database.get("name2");

        database.count("michal");
        database.commit();
        database.rollback();
    }

}
