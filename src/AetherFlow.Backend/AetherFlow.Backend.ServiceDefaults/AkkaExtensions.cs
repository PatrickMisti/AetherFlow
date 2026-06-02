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
            var actorSystemName = builder.Configuration["Akka:ActorSystemName"] ?? "AetherFlowCluster";
            // Alternative: require an actor system name from configuration
            // var actorSystemName = builder.Configuration["Akka:ActorSystemName"]
            //     ?? throw new InvalidOperationException("Akka:ActorSystemName must be provided by the host.");
            var settings = GetSettings(builder.Configuration);

            builder.Services.AddAkka(actorSystemName, config =>
            {
                var configured = config
                    .AddLogging()
                    .AddRemote(settings.Remote.Host, settings.Remote.Port)
                    .AddClustering(settings.Cluster);
                // .AddDiscovery(settings) // enable cluster discovery (DNS / Kubernetes) in dynamic environments
                // .WithAkkaManagement()   // configure Akka Management when using discovery/bootstrap

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
            // Useful when running in dynamic environments such as Kubernetes or Azure (enables dynamic discovery).
            => builder
                .WithAkkaManagement(port: settings.Management.Port, hostName: settings.Remote.Host)
                // If you need to bind a specific hostname/port (e.g. behind NAT or in Docker), set:
                // opt.Http.HostName = settings.Remote.Host;
                // opt.Http.Port = settings.Management.Port;
                // bindHost and port if behind NAT or in Docker bridge
                .WithClusterBootstrap(opt =>
                {
                    opt.ContactPointDiscovery.ServiceName = settings.Cluster.ServiceName;
                    opt.ContactPointDiscovery.RequiredContactPointsNr = settings.Cluster.RequiredContactPoints;
                    // Force bootstrap to use the 'config' discovery method instead of any system default.
                    opt.ContactPointDiscovery.DiscoveryMethod = "config";

                    // Required in a localhost environment when both processes run on the same host but use different ports.
                    opt.ContactPoint.FilterOnFallbackPort = false;
                })
                .WithConfigDiscovery(opt =>
                {
                    // Register discovered services for ConfigServiceDiscovery. Each node can add its management endpoint
                    // so Cluster Bootstrap can discover contact points for joining.
                    opt.Services.Add(new Service
                    {
                        Name = settings.Cluster.ServiceName,
                        // Endpoints = [$"{settings.Remote.Host}:{settings.Management.Port}"]
                        // For DNS-based discovery, SRV records (host + port) are preferable.
                        // Hardcoded endpoints (below) are only for local testing:
                        Endpoints = ["localhost:8890", "localhost:8891"]
                    });
                });

        private AkkaConfigurationBuilder AddRemote(string host, int port)
            => builder.WithRemoting(opt =>
            {
                opt.HostName = host;
                opt.Port = port;
            });

        /// <summary>
        /// Applies cluster roles, optional static seed nodes, minimum member gating, and split-brain behavior.
        /// <para>
        /// <c>MinimumNumberOfMembers</c> only delays the cluster state transition to <c>Up</c>; it does not stop local actor
        /// execution before the cluster reaches <c>Up</c>.
        /// </para>
        /// <para>
        /// <c>SplitBrainResolver</c> is set to <c>null</c> here, which disables automatic split-brain downing in this default path.
        /// If your environment requires an explicit split-brain strategy, configure it through HOCON or environment-specific
        /// Akka hosting settings.
        /// </para>
        /// </summary>
        /// <param name="cs">Cluster settings loaded from configuration.</param>
        /// <returns>The same <see cref="AkkaConfigurationBuilder"/> instance for fluent chaining.</returns>
        private AkkaConfigurationBuilder AddClustering(AkkaSettings.ClusterSettings cs)
            => builder.WithClustering(new ClusterOptions
            {
                Roles = cs.Roles,
                SeedNodes = cs.SeedNodes,
                // SplitBrainResolver = SplitBrainResolverOption.Default
                // SplitBrainResolver = new KeepMajorityOption()
                // SplitBrainResolver = new StaticQuorumOption()
                // {
                //     QuorumSize = 1
                // }
                // SplitBrainResolver = new KeepOldestOption{DownIfAlone = false}
                // // Disable automatic split-brain resolution to prevent unintended node downing during development/testing
                SplitBrainResolver = null,
            });

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