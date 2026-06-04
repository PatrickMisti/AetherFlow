using AetherFlow.Backend.ServiceDefaults;
using AetherFlow.Engine.Actors;
using AetherFlow.Shared.AetherInterfaces;
using AetherFlow.Shared.Config;
using Akka.Actor;
using Akka.DependencyInjection;
using Akka.Hosting;
using Akka.Persistence.Sql.Hosting;

namespace AetherFlow.Engine.ApplicationBuilderConfig;

public static class ServiceBuilderConfig
{
    extension<T>(T builder) where T : IHostApplicationBuilder
    {
        /// <summary>
        /// Registers the Akka.NET cluster with a shard region that hosts <see cref="AetherEngineActor"/> entities.
        /// Shard region name and role are driven by <c>Akka:Cluster</c> configuration — the node must have
        /// the matching role in <c>Akka:Cluster:Roles</c> to host shards.
        /// </summary>
        public T WithEngineWorker()
        {
            return builder.AddAkkaDefaults((config, settings) =>
            {
                config.AddShardRegion<IAetherShardMarker>(
                    settings: settings,
                    (di) => di.Props<AetherEngineActor>());
                config.AddPersistence(builder.Configuration);
            });
        }
    }

    extension(AkkaConfigurationBuilder config)
    {
        private AkkaConfigurationBuilder AddPersistence(IConfigurationManager manager)
        {
            var dbConfig = manager.GetSection("AkkaDbSettings").Get<AkkaDbSettings>() ?? new AkkaDbSettings();

            config.WithSqlPersistence(
                connectionString: dbConfig.ConnectionString,
                providerName: dbConfig.ProviderName,
                autoInitialize: true);
            
            return config;
        }
    }
}