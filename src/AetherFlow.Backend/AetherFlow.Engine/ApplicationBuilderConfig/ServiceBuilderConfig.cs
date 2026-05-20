using AetherFlow.Backend.ServiceDefaults;
using AetherFlow.Engine.Actors;
using AetherFlow.Shared.AetherInterfaces;
using Akka.Actor;

namespace AetherFlow.Engine.ApplicationBuilderConfig;

public static class ServiceBuilderConfig
{
    extension<T>(T builder) where T : IHostApplicationBuilder
    {
        public T WithEngineWorker()
        {
            return builder.AddAkkaDefaults((config, settings) =>
            {
                config.AddShardRegion<IAetherShardMarker>(
                    typeName: settings.Cluster.Roles.FirstOrDefault() ?? settings.Cluster.ServiceName,
                    props => Props.Create(() => new AetherEngineActor(props)));
            });
        }
    }
}