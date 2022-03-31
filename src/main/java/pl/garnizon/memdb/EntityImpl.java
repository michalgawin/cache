package pl.garnizon.memdb;

import java.util.Comparator;
import java.util.Objects;

public class EntityImpl<T extends Comparable<T>> implements Entity<T> {

    T value;

    public EntityImpl(T value) {
        this.value = value;
    }

    @Override
    public T getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EntityImpl<?> entity = (EntityImpl<?>) o;
        return Objects.equals(getValue(), entity.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getValue());
    }

    @Override
    public String toString() {
        return "" + value;
    }

    @Override
    public int compareTo(Entity<T> o) {
        if (Objects.isNull(o)) {
            return 1;
        }
        if (Objects.isNull(getValue()) || Objects.isNull(o.getValue())) {
            if (Objects.isNull(getValue())) {
                return -1;
            } else if (Objects.isNull(o.getValue())) {
                return 1;
            } else {
                return 0;
            }
        }

        return Objects.compare(this, o, Comparator.comparing(Entity::getValue));
    }
}
