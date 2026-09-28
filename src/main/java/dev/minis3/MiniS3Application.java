package dev.minis3;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * One codebase, three roles. The active Spring profile decides what this
 * process is:
 *
 *   api     - public REST API; places and replicates objects (talks to Postgres + nodes)
 *   storage - dumb blob store on local disk; sends heartbeats to the API
 *   worker  - background janitor: failure detection, re-replication, garbage collection
 */
@SpringBootApplication
@EnableScheduling
public class MiniS3Application {
    public static void main(String[] args) {
        SpringApplication.run(MiniS3Application.class, args);
    }
}
