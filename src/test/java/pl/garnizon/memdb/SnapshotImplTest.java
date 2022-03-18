package pl.garnizon.memdb;

import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SnapshotImplTest {

    @Mock
    Map<String, String> memtable;

    @Mock
    Multimap<String, String> invertedIndex;

    @InjectMocks
    SnapshotImpl<String> snapshot;

    @Test
    public void setTest() {
        snapshot.set("key", "value");

        verify(memtable).put(any(), any());
    }

    @Test
    public void getTest() {
        when(memtable.getOrDefault(any(), any())).thenReturn("abc");

        assertThat(snapshot.get("def")).isEqualTo("abc");

        verify(memtable).getOrDefault(any(), any());
    }

    @Test
    public void deleteTest() {
        snapshot.delete("def");

        verify(memtable).put(any(), any());
    }

    @Test
    public void countTest() {
        when(memtable.get("a")).thenReturn("c");
        when(memtable.get("b")).thenReturn("c");
        when(invertedIndex.get("c")).thenReturn(Set.of("a", "b"));
        when(invertedIndex.get("a")).thenReturn(Set.of());
        when(invertedIndex.get(SnapshotImpl.getTombstone())).thenReturn(Set.of());

        assertThat(snapshot.count("c")).isEqualTo(2);
        assertThat(snapshot.count("a")).isEqualTo(0);
    }

    @Test
    public void mergeTest() {
        Snapshot snapshotMock = mock(Snapshot.class);
        when(snapshotMock.getSnapshot()).thenReturn(Map.of("a", "b"));

        snapshot.merge(snapshotMock);

        verify(memtable).put(anyString(), anyString());
    }

}
