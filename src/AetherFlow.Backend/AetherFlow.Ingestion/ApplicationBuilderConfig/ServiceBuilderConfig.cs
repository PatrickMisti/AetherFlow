using AetherFlow.Backend.ServiceDefaults;
using AetherFlow.Domain.Domains;
using AetherFlow.Infrastructure.AetherGenerator;
using AetherFlow.Ingestion.Actors;
using AetherFlow.Shared.AetherInterfaces;
using AetherFlow.Shared.Messages.Ingestion;
using Akka.Actor;
using Akka.Hosting;
using Akka.Net.Collector.Agent;

namespace AetherFlow.Ingestion.ApplicationBuilderConfig;

internal static class ServiceBuilderConfig
{
    extension<T>(T builder) where T : IHostApplicationBuilder
    {
        public T UseAkka()
        {
            builder.AddAkkaDefaults((config, settings) =>
            {
                config.AddShardRegionProxy<IAetherShardProxyMarker>(settings: settings);
                config.AddMonitoringAgent();
                // system -> actor-system
                // registry -> out of system // DI container for actors // only for node not for cluster
                // di -> into the system

                // registry -> actor can be used outside the actor-system // with IActorRegistry -> GetAsync<T>
                // registry -> same with IActorRegistry in actor to use another actor in the system

                // di -> not only into the system also needed to inject into actor
                // registry to find actor + di to inject actor or class
                config.WithActors((system, registry, di) =>
                {
                    var pipActor = system.ActorOf(di.Props<AetherPipelineActor>(settings.Cluster.ShardRegionRole));
                    var genSupervisor = system.ActorOf(di.Props<AetherSupervisorActor>());

                    registry.Register<AetherSupervisorActor>(genSupervisor);
                    registry.Register<AetherPipelineActor>(pipActor);

                    genSupervisor.Tell(new StartGenerator());
                });
            });

            return builder;
        }

        public T AddServices()
        {
            builder.Services.AddScoped<IPeripheryConnector<AetherChunk>, AetherGeneratorSensor>();
            return builder;
        }
    }
}