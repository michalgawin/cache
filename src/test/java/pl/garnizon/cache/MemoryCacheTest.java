package pl.garnizon.cache;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import pl.garnizon.cache.api.TransactionalDatabase;

class MemoryCacheTest {

    @Test
    public void allIn() {
        TransactionalDatabase<String> database = MemoryCache.create();
        database.set("name", "abc")
                .get("name");
        database.delete("name")
                .get("name");
        database.set("name", "abc")
                .set("name2", "abc")
                .count("abc");
        database.begin();
        database.set("name", "ghi");
        database.begin();
        database.set("name2", "def")
                .set("name3", "abc");
        database.rollback();
        database.set("name4", "abc");
        database.commit();
        database.begin();
        database.delete("name4");
        database.begin();
        database.delete("name2");
        database.commit()
                .commit();
        database.get("name");
        database.get("name2");

        database.count("abc");
        database.commit();
        database.rollback();

        Assertions.assertThat(database.count("abc")).isEqualTo(0);
        Assertions.assertThat(database.count("def")).isEqualTo(0);
        Assertions.assertThat(database.count("ghi")).isEqualTo(1);
    }

}
