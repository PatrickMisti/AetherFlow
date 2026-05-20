using AetherFlow.Infrastructure.AetherShardRegion;
using AetherFlow.Shared.Config;
using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding;
using Akka.Discovery.Config.Hosting;
using Akka.Hosting;
using Akka.Logger.Serilog;
using Akka.Management;
using Akka.Management.Cluster.Bootstrap;
using Akka.Remote.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace AetherFlow.Backend.ServiceDefaults;

public static class AkkaExtensions
{
    private static AkkaSettings GetSettings(IConfigurationManager config)
        => config.GetSection("Akka").Get<AkkaSettings>() ?? new AkkaSettings();

    extension<TBuilder>(TBuilder builder) where TBuilder : IHostApplicationBuilder
    {
        public TBuilder AddAkkaDefaults(Action<AkkaConfigurationBuilder, AkkaSettings>? additionalConfig = null)
        {
            var actorSystemName = builder.Configuration["Akka:ActorSystemName"]
                                  ?? throw new InvalidOperationException(
                                      "Akka:ActorSystemName must be provided by AppHost.");
            var settings = GetSettings(builder.Configuration);

            builder.Services.AddAkka(actorSystemName, config =>
            {
                var configured = config
                    .AddLogging()
                    .AddRemote(settings.Remote.Host, settings.Remote.Port)
                    .AddClustering(settings.Cluster.Roles)
                    .AddDiscovery(settings);

                additionalConfig?.Invoke(configured, settings);
            });

            return builder;
        }

        public TBuilder AddAkkaDefaults(Action<AkkaConfigurationBuilder>? additionalConfig = null)
            => AddAkkaDefaults(
                builder,
                (config, _) => additionalConfig?.Invoke(config));
    }

    extension(AkkaConfigurationBuilder builder)
    {
        private AkkaConfigurationBuilder AddLogging()
            => builder.ConfigureLoggers(opt =>
            {
                opt.ClearLoggers();
                opt.AddSerilogLogging();
            });

        private AkkaConfigurationBuilder AddDiscovery(AkkaSettings settings)
            => builder
                .WithAkkaManagement(opt =>
                {
                    opt.Http.HostName = settings.Remote.Host;
                    opt.Http.Port = settings.Management.Port;
                })
                .WithClusterBootstrap(opt =>
                {
                    opt.ContactPointDiscovery.ServiceName = settings.Cluster.ServiceName;
                    opt.ContactPointDiscovery.RequiredContactPointsNr = settings.Cluster.RequiredContactPoints;
                })
                .WithConfigDiscovery(opt =>
                {
                    // every node register itself- Bootstrap make cluster-joining
                    opt.Services.Add(new Service
                    {
                        Name = settings.Cluster.ServiceName,
                        Endpoints = [$"{settings.Remote.Host}:{settings.Management.Port}"]
                    });
                });

        private AkkaConfigurationBuilder AddRemote(string host, int port)
            => builder.WithRemoting(opt =>
            {
                opt.HostName = host;
                opt.Port = port;
            });

        private AkkaConfigurationBuilder AddClustering(string[] roles)
            => builder.WithClustering(new ClusterOptions { Roles = roles });

        public AkkaConfigurationBuilder AddShardRegion<TMarker>(
            string typeName,
            Func<string, Props> entityPropsFactory,
            IMessageExtractor? messageExtractor = null,
            ShardOptions? shardOptions = null)
            where TMarker : IClusterShardingSerializable
            => builder.WithShardRegion<TMarker>(
                typeName: typeName,
                entityPropsFactory: entityPropsFactory,
                messageExtractor: messageExtractor ?? CustomMessageExtractor.Create(),
                shardOptions: shardOptions ?? new()
                {
                    Role = typeName,
                    StateStoreMode = StateStoreMode.DData,
                    PassivateIdleEntityAfter = TimeSpan.FromMinutes(2)
                });
        // Extractor from Akka.net
        /*HashCodeMessageExtractor.Create(maxNumberOfShards: 100,
        entityIdExtractor: msg => msg switch
        {
            _ => null
        }),*/
    }
}