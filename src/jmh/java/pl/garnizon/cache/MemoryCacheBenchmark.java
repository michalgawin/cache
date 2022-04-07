package pl.garnizon.cache;

import org.assertj.core.api.Assertions;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import pl.garnizon.cache.api.TransactionalDatabase;

public class MemoryCacheBenchmark {

    @Benchmark
    @Fork(3)
    @Warmup(iterations = 5)
    @BenchmarkMode({ Mode.All })
    public void benchmarkOperations(Blackhole blackhole) {
        TransactionalDatabase<String> cache = MemoryCache.create();
        someOperations(cache);
        blackhole.consume(cache);
    }

    private TransactionalDatabase<String> someOperations(TransactionalDatabase<String> cache) {
        cache.set("name", "abc")
                .get("name");
        cache.delete("name")
                .get("name");
        cache.set("name", "abc")
                .set("name2", "abc")
                .count("abc");
        cache.begin();
        cache.set("name", "ghi");
        cache.begin();
        cache.set("name2", "def")
                .set("name3", "abc");
        cache.rollback();
        cache.set("name4", "abc");
        cache.commit();
        cache.begin();
        cache.delete("name4");
        cache.begin();
        cache.delete("name2");
        cache.commit()
                .commit();
        cache.get("name");
        cache.get("name2");

        cache.count("abc");
        cache.commit();
        cache.rollback();

        cache.count("abc");
        cache.count("def");
        cache.count("ghi");

        return cache;
    }

}
