using AetherFlow.Infrastructure.AetherShardRegion;
using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding;
using Akka.Hosting;
using Akka.Logger.Serilog;
using Akka.Remote.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace AetherFlow.Backend.ServiceDefaults;

public static class AkkaExtensions
{
    private static (string Host, int Port, string Role, string[] SeedNodes) GetConfig(this ConfigurationManager manager)
    {
        var host = manager["Cluster:Host"] ??
                   throw new InvalidOperationException("Host is not configured.");
        if (!int.TryParse(manager["Cluster:Port"], out var port))
            throw new InvalidOperationException("Port is not a valid integer.");
        var role = manager["Cluster:Role"] ?? throw new InvalidOperationException("Role is not configured.");
        var seedNodes = manager["Akka:SeedNodes"] ??
                        throw new InvalidOperationException("SeedNodes is not configured.");

        return (
            Host: host,
            Port: port,
            Role: role,
            SeedNodes: seedNodes
                .Split(',')
                .Select(node => node.Trim()).ToArray());
    }

    extension<TBuilder>(TBuilder builder) where TBuilder : IHostApplicationBuilder
    {
        public TBuilder AddAkkaDefaults()
        {
            var akkaActorSystemName = builder.Configuration["Akka:ActorSystemName"] ??
                                      throw new InvalidOperationException("Akka Actor System Name is not configured.");

            builder.Services.AddAkka(akkaActorSystemName, config =>
            {
                config.AddLogging()
                    .AddRemote(host: "localhost", port: 9090)
                    .AddClustering(roles: new[] { "aether-engine" },
                        seedNodes: new[] { $"akka.tcp://{akkaActorSystemName}@localhost:9090" });
            });

            return builder;
        }
    }

    extension(AkkaConfigurationBuilder builder)
    {
        public AkkaConfigurationBuilder AddLogging()
        {
            builder.ConfigureLoggers(opt =>
            {
                opt.ClearLoggers();
                opt.AddSerilogLogging();
            });

            return builder;
        }

        public AkkaConfigurationBuilder AddRemote(string host, int port)
        {
            builder.WithRemoting(opt =>
            {
                opt.HostName = host;
                opt.Port = port;
            });

            return builder;
        }

        public AkkaConfigurationBuilder AddClustering(string[] roles, string[] seedNodes)
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