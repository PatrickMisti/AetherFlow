using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Hosting;
using Akka.Net.Collector.Contracts;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Akka.Net.Collector.Collector;

/// <summary>
/// Wires up the collector side: the <see cref="CollectorActor"/> plus the HTTP API that
/// exposes the gathered snapshots. Cluster/seed configuration comes from the ActorSystem's
/// HOCON — nothing is hardcoded here.
/// </summary>
public static class MonitoringCollectorExtensions
{
    public static AkkaConfigurationBuilder AddMonitoringCollector(this AkkaConfigurationBuilder builder) =>
        builder
            .WithDistributedPubSub(role: "")
            .WithActors((system, registry) =>
            {
                var collector = system.ActorOf(Props.Create<CollectorActor>(), "monitoring-collector");
                registry.Register<CollectorActor>(collector);
            });

    /// <summary>Maps <c>GET {prefix}/nodes</c> returning the latest snapshot per monitored node.</summary>
    public static IEndpointRouteBuilder MapMonitoringApi(
        this IEndpointRouteBuilder endpoints,
        string prefix = "/monitoring")
    {
        endpoints.MapGet($"{prefix}/nodes", async (ActorRegistry registry) =>
        {
            var collector = registry.Get<CollectorActor>();
            var snapshots = await collector.Ask<AgentSnapshot[]>(
                CollectorActor.GetAll.Instance, TimeSpan.FromSeconds(3));
            return Results.Ok(snapshots);
        });

        return endpoints;
    }
}
