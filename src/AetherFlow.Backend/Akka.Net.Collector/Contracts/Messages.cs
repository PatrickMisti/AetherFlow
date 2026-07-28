namespace Akka.Net.Collector.Contracts;

/// <summary>A single actor in the hierarchy snapshot.</summary>
public sealed record ActorNode(string Path, string Name, IReadOnlyList<ActorNode> Children);

/// <summary>Lifecycle counters gathered via Akka actor telemetry.</summary>
public sealed record MetricCounts(int Started, int Stopped, int Restarted);

/// <summary>Everything one monitored node reports in a single push.</summary>
public sealed record AgentSnapshot(
    string NodeAddress,
    ActorNode Root,
    IReadOnlyList<string> Events,
    MetricCounts Metrics);

public static class MonitoringTopics
{
    public const string Snapshots = "monitoring-snapshots";
}
