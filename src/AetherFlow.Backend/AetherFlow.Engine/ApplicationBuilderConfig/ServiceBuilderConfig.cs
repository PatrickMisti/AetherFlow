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
            return builder.AddAkkaDefaults((config, akkaConfig) =>
            {
                config.AddShardRegion<IAetherShardMarker>(
                    typeName: akkaConfig.ActiveRole,
                    props => Props.Create(() => new AetherEngineActor(props)));
            });
        }
    }
}