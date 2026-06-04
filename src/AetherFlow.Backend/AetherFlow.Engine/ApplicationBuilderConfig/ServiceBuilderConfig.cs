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
                    settings: settings,
                    props => Props.Create(() => new AetherEngineActor(props)));
            });
        }
    }
}