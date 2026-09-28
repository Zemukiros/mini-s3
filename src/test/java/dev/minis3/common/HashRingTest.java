package dev.minis3.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

/**
 * These tests prove the two properties that justify consistent hashing:
 * balanced load, and minimal data movement when the cluster changes.
 */
class HashRingTest {

    private static final int KEYS = 100_000;

    @Test
    void preferenceListReturnsDistinctNodes() {
        HashRing ring = new HashRing(List.of("a", "b", "c", "d"), 100);
        for (int i = 0; i < 1000; i++) {
            List<String> pref = ring.preferenceList(UUID.randomUUID().toString(), 3);
            assertEquals(3, pref.size());
            assertEquals(3, new HashSet<>(pref).size());
        }
    }

    @Test
    void neverReturnsMoreNodesThanExist() {
        HashRing ring = new HashRing(List.of("a", "b"), 100);
        assertEquals(2, ring.preferenceList("x", 3).size());
    }

    @Test
    void virtualNodesBalanceLoad() {
        List<String> nodes = List.of("node-1", "node-2", "node-3", "node-4", "node-5");
        double withOne = maxDeviation(new HashRing(nodes, 1), nodes.size());
        double withMany = maxDeviation(new HashRing(nodes, 200), nodes.size());
        System.out.printf("Max deviation from fair share: 1 vnode = %.0f%%, 200 vnodes = %.0f%%%n",
                withOne * 100, withMany * 100);
        assertTrue(withMany < 0.20, "200 vnodes should keep every node within 20% of fair share");
        assertTrue(withMany < withOne);
    }

    @Test
    void addingANodeMovesAboutOneNthOfKeys() {
        HashRing before = new HashRing(List.of("n1", "n2", "n3", "n4"), 200);
        HashRing after = new HashRing(List.of("n1", "n2", "n3", "n4", "n5"), 200);
        int moved = 0;
        for (int i = 0; i < KEYS; i++) {
            String k = "key-" + i;
            if (!before.preferenceList(k, 1).equals(after.preferenceList(k, 1))) {
                moved++;
            }
        }
        double fraction = moved / (double) KEYS;
        // Ideal is 1/5 = 20%. Naive hash % N would move ~80%.
        System.out.printf("Adding 5th node moved %.1f%% of keys (hash %% N would move ~80%%)%n", fraction * 100);
        assertTrue(fraction > 0.12 && fraction < 0.28);
    }

    @Test
    void removingANodeOnlyMovesThatNodesKeys() {
        HashRing before = new HashRing(List.of("n1", "n2", "n3", "n4"), 100);
        HashRing after = new HashRing(List.of("n1", "n2", "n3", "n4"), 100);
        after.removeNode("n4");
        for (int i = 0; i < KEYS; i++) {
            String k = "key-" + i;
            String owner = before.preferenceList(k, 1).get(0);
            if (!owner.equals("n4")) {
                assertEquals(owner, after.preferenceList(k, 1).get(0), "keys not on n4 must not move");
            }
        }
    }

    private static double maxDeviation(HashRing ring, int nodeCount) {
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < KEYS; i++) {
            counts.merge(ring.preferenceList("key-" + i, 1).get(0), 1, Integer::sum);
        }
        double fair = KEYS / (double) nodeCount;
        return counts.values().stream().mapToDouble(c -> Math.abs(c - fair) / fair).max().orElse(0);
    }
}
