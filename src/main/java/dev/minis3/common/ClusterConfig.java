package dev.minis3.common;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/** Beans shared by the two roles that coordinate replication: api and worker. */
@Configuration
@Profile({"api", "worker"})
public class ClusterConfig {

    @Bean
    StorageClient storageClient() {
        return new StorageClient();
    }

    @Bean
    Placement placement(MetadataRepository repo,
                        @Value("${minis3.virtual-nodes}") int virtualNodes) {
        return new Placement(repo, virtualNodes);
    }

    /**
     * Builds a hash ring from the currently UP nodes and answers "where should
     * copies of this object go?". Rebuilt per call: with a handful of nodes
     * that's a few hundred hashes, which costs microseconds and means the ring
     * can never be out of date with the nodes table.
     */
    public static class Placement {
        private final MetadataRepository repo;
        private final int virtualNodes;

        Placement(MetadataRepository repo, int virtualNodes) {
            this.repo = repo;
            this.virtualNodes = virtualNodes;
        }

        public List<Node> upNodes() {
            return repo.upNodes();
        }

        /** Preference list over the given nodes, in ring order. */
        public List<Node> preferenceList(UUID objectId, List<Node> nodes, int n) {
            Map<String, Node> byId = nodes.stream().collect(Collectors.toMap(Node::id, Function.identity()));
            HashRing ring = new HashRing(byId.keySet(), virtualNodes);
            return ring.preferenceList(objectId.toString(), n).stream().map(byId::get).toList();
        }
    }
}
