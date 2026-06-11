namespace AetherFlow.TestSupport;

/// <summary>
/// HOCON snippets for TestKit fixtures that need a (single-node) cluster, optionally with
/// persistence. Centralised so the pipeline and engine actor tests don't each hand-roll config.
/// </summary>
public static class TestKitConfigs
{
    /// <summary>Default cluster role used by tests and matched against <c>shardRegionRole</c>.</summary>
    public const string DefaultRole = "shard-region";

    /// <summary>Cluster provider + ephemeral remoting, with a single configurable role.</summary>
    public static string Cluster(string role = DefaultRole) => $$"""
        akka {
            loglevel = WARNING
            actor.provider = cluster
            remote.dot-netty.tcp {
                hostname = "127.0.0.1"
                port = 0
            }
            cluster.roles = ["{{role}}"]
        }
        """;

    /// <summary>
    /// Cluster config plus an in-memory journal and a local snapshot store pointed at a unique
    /// temp directory (so persistent-actor tests are isolated and self-cleaning per actor system).
    /// </summary>
    public static string ClusterWithPersistence(string role = DefaultRole)
    {
        var snapshotDir = Path.Combine(Path.GetTempPath(), "aetherflow-test-snapshots", Guid.NewGuid().ToString("N"))
            .Replace("\\", "/");

        return Cluster(role) + $$"""

            akka.persistence {
                journal.plugin = "akka.persistence.journal.inmem"
                snapshot-store.plugin = "akka.persistence.snapshot-store.local"
                snapshot-store.local.dir = "{{snapshotDir}}"
            }
            """;
    }
}
