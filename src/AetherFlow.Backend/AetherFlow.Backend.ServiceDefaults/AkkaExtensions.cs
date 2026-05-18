using AetherFlow.Infrastructure.AetherShardRegion;
using AetherFlow.Shared.Config;
using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding;
using Akka.Discovery.Config.Hosting;
using Akka.Hosting;
using Akka.Logger.Serilog;
using Akka.Management;
using Akka.Remote.Hosting;
using Grpc.Net.Client.Configuration;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace AetherFlow.Backend.ServiceDefaults;

public static class AkkaExtensions
{
    private static AkkaInfConfig GetConfig(this IConfigurationManager manager)
    {
        var host = manager["Akka:Cluster:Host"] ??
                   throw new InvalidOperationException("Host is not configured.");
        if (!int.TryParse(manager["Akka:Cluster:Port"], out var port))
            throw new InvalidOperationException("Port is not a valid integer.");
        var roles = manager.GetSection("Akka:Cluster:Roles").GetChildren().Select(v => v.Value!).ToArray() 
                    ?? throw new InvalidOperationException("Role is not configured.");
        var seedNodes = manager.GetSection("Akka:SeedNodes").GetChildren().Select(v => v.Value!).ToArray()
                        ?? throw new InvalidOperationException("SeedNodes is not configured.");

        var first = roles.FirstOrDefault() ??
                    throw new InvalidOperationException("At least one role must be configured.");
        return new AkkaInfConfig(
            Host: host,
            Port: port,
            Roles: roles,
            SeedNodes: seedNodes,
            ActiveRole: first);
    }

    extension<TBuilder>(TBuilder builder) where TBuilder : IHostApplicationBuilder
    {
        public TBuilder AddAkkaDefaults(Action<AkkaConfigurationBuilder, AkkaInfConfig>? additionalConfig = null)
        {
            var akkaActorSystemName = builder.Configuration["Akka:ActorSystemName"] ??
                                      throw new InvalidOperationException("Akka Actor System Name is not configured.");
            var akkaConfig = GetConfig(builder.Configuration);
            builder.Services.AddAkka(akkaActorSystemName, config =>
            {
                config.AddLogging()
                    .AddRemote(host: akkaConfig.Host, port: akkaConfig.Port)
                    .AddClustering(roles: akkaConfig.Roles, seedNodes: akkaConfig.SeedNodes)
                    .AddDiscovery(akkaConfig);

                additionalConfig?.Invoke(config, akkaConfig);
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
        {
            builder.ConfigureLoggers(opt =>
            {
                opt.ClearLoggers();
                opt.AddSerilogLogging();
            });

            return builder;
        }
        
        private AkkaConfigurationBuilder AddDiscovery(AkkaInfConfig akka)
        {
            builder.WithAkkaManagement(opt =>
            {
                opt.Http.HostName = akka.Host;
                opt.Http.Port = 8888;
            }).WithConfigDiscovery(opt =>
            {
                opt.Services.Add(new Service
                {
                    Name = "aether-engine",
                    Endpoints = akka.SeedNodes
                });
            });

            return builder;
        }

        private AkkaConfigurationBuilder AddRemote(string host, int port)
        {
            builder.WithRemoting(opt =>
            {
                opt.HostName = host;
                opt.Port = port;
            });

            return builder;
        }

        private AkkaConfigurationBuilder AddClustering(string[] roles, string[] seedNodes)
        {
            builder.WithClustering(new()
            {
                Roles = roles,
                SeedNodes = seedNodes
            });

            return builder;
        }

        public AkkaConfigurationBuilder AddShardRegion<TMarker>(
            string typeName,
            Func<string, Props> entityPropsFactory,
            IMessageExtractor? messageExtractor = null,
            ShardOptions? settings = null)
            where TMarker : IClusterShardingSerializable
        {
            builder.WithShardRegion<TMarker>(
                typeName: typeName,
                entityPropsFactory: entityPropsFactory,
                messageExtractor: messageExtractor ?? CustomMessageExtractor.Create(),
                shardOptions: settings ?? new()
                {
                    Role = typeName,
                    StateStoreMode = StateStoreMode.DData,
                    // RememberEntities = true, // not in combination with PassivateIdleEntityAfter
                    PassivateIdleEntityAfter = TimeSpan.FromMinutes(2)
                }
            );
            // Extractor from Akka.net
            /*HashCodeMessageExtractor.Create(maxNumberOfShards: 100,
            entityIdExtractor: msg => msg switch
            {
                _ => null
            }),*/

            return builder;
        }
    }
}