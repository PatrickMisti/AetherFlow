using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Hosting;

namespace Akka.Net.Collector.Agent;

/// <summary>
/// Registers the in-process monitoring agent. Call this on the ActorSystem of the app you
/// want to observe. No host/port/address needed — the agent reuses the cluster the system
/// is already part of and publishes via DistributedPubSub.
/// </summary>
public static class MonitoringAgentExtensions
{
    public static AkkaConfigurationBuilder AddMonitoringAgent(this AkkaConfigurationBuilder builder) =>
        builder
            // enable Akka actor telemetry so lifecycle counters are populated
            .AddHocon("akka.actor.telemetry.enabled = on", HoconAddMode.Prepend)
            .WithDistributedPubSub(role: "")
            .WithActors((system, registry) =>
            {
                var agent = system.ActorOf(Props.Create<MonitoringAgentActor>(), "monitoring-agent");
                registry.Register<MonitoringAgentActor>(agent);
            });
}

