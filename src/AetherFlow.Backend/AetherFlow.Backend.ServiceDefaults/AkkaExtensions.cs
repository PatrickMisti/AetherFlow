using AetherFlow.Infrastructure.Actors;
using AetherFlow.Infrastructure.AetherShardRegion;
using AetherFlow.Shared.Config;
using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding;
using Akka.DependencyInjection;
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
        public AkkaConfigurationBuilder AddNotifier() => builder.WithActors((system, registry, di) =>
            registry.TryRegister<NotifyHandler>(system.ActorOf(di.Props<NotifyHandler>())));
      
        /// <summary>
        /// Add Logging to Akka.NET using Serilog.
        /// This is a common choice for structured logging in .NET applications and integrates well with various logging sinks.
        /// </summary>
        /// <returns></returns>
        private AkkaConfigurationBuilder AddLogging()
            => builder.ConfigureLoggers(opt =>
            {
                opt.ClearLoggers();
                opt.AddSerilogLogging();
            });

        /// <summary>
        /// Configures Akka.Remote with the specified host and port.
        /// This enables remote actor communication and is required for clustering.
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Configures and registers a shard region for distributed actor sharding in the Akka.NET cluster.
        /// <para>
        /// Uses <see cref="AkkaSettings.ClusterSettings.ShardRegionName"/> as the shard region identifier and
        /// <see cref="AkkaSettings.ClusterSettings.ShardRegionRole"/> as the cluster role. The role must match
        /// one of the roles in <see cref="AkkaSettings.ClusterSettings.Roles"/> on every node intended to host shards.
        /// </para>
        /// <para>
        /// By default, this method configures:
        /// <list type="bullet">
        /// <item><description>Event-sourced persistence state store mode for shard coordinator resilience</description></item>
        /// <item><description>Automatic entity passivation after 2 minutes of inactivity</description></item>
        /// <item><description>Remember entities via event-sourced store so entity IDs survive rebalancing and shutdown</description></item>
        /// <item><description>Role set from <see cref="AkkaSettings.ClusterSettings.ShardRegionRole"/></description></item>
        /// </list>
        /// </para>
        /// </summary>
        /// <typeparam name="TMarker">A marker type implementing <see cref="IClusterShardingSerializable"/> used to identify the shard region type.</typeparam>
        /// <param name="settings">Akka settings providing the shard region name and role from <c>Cluster</c> configuration.</param>
        /// <param name="entityPropsFactory">A factory function that creates <see cref="Props"/> for entities based on the entity ID. Called for each entity instantiation.</param>
        /// <param name="messageExtractor">Optional message extractor to determine entity ID and shard ID from messages. Defaults to <see cref="CustomMessageExtractor"/> if not provided.</param>
        /// <param name="shardOptions">Optional shard configuration options. When provided, <c>Role</c> is still derived from settings unless the caller sets it explicitly before passing.</param>
        /// <returns>The same <see cref="AkkaConfigurationBuilder"/> instance for fluent chaining.</returns>
        public AkkaConfigurationBuilder AddShardRegion<TMarker>(
            AkkaSettings settings,
            Func<ActorSystem, IActorRegistry, IDependencyResolver, Func<string, Props>> entityPropsFactory,
            IMessageExtractor? messageExtractor = null,
            ShardOptions? shardOptions = null)
            where TMarker : IClusterShardingSerializable
            => builder.WithShardRegion<TMarker>(
                typeName: settings.Cluster.ShardRegionName,
                entityPropsFactory: entityPropsFactory,
                messageExtractor: messageExtractor ?? CustomMessageExtractor.Create(),
                shardOptions: shardOptions ?? new()
                {
                    Role = settings.Cluster.ShardRegionRole,
                    StateStoreMode = StateStoreMode.Persistence,
                    PassivateIdleEntityAfter = TimeSpan.FromMinutes(2),
                    // so after shutdown or rebalancing actor info isn't lost
                    // without after passivation entity id would be lost only grab id with new incoming msg
                    RememberEntities = false,
                    // ddata only remembers CRDT (Distributed Data) not state with eventsourced usage of journal
                    RememberEntitiesStore = RememberEntitiesStore.Eventsourced, // todo test if needed
                    ShouldPassivateIdleEntities = true
                });

        public AkkaConfigurationBuilder AddShardRegion<TMarker>(
            AkkaSettings settings,
            Func<IDependencyResolver, Props> entityPropsFactory,
            IMessageExtractor? messageExtractor = null,
            ShardOptions? shardOptions = null)
            where TMarker : IClusterShardingSerializable
            => builder.AddShardRegion<TMarker>(
                settings: settings,
                entityPropsFactory: (_, _, di) => (_) => entityPropsFactory.Invoke(di),
                messageExtractor: messageExtractor,
                shardOptions: shardOptions);


        // Extractor from Akka.net
        /*HashCodeMessageExtractor.Create(maxNumberOfShards: 100,
        entityIdExtractor: msg => msg switch
        {
            _ => null
        }),*/

        /// <summary>
        /// Registers a shard region proxy on this node, allowing it to forward messages to shard entities
        /// hosted on other nodes without hosting any shards itself.
        /// <para>
        /// Uses <see cref="AkkaSettings.ClusterSettings.ShardRegionName"/> as the shard region identifier and
        /// <see cref="AkkaSettings.ClusterSettings.ShardRegionRole"/> as the target role. The proxy buffers
        /// messages internally until a node with the matching role is <c>Up</c> in the cluster.
        /// </para>
        /// </summary>
        /// <typeparam name="TMarker">A marker type implementing <see cref="IClusterShardingSerializable"/> used to resolve the proxy via <c>IActorRegistry</c>.</typeparam>
        /// <param name="settings">Akka settings providing the shard region name and role from <c>Cluster</c> configuration.</param>
        /// <param name="messageExtractor">Optional message extractor to determine entity ID and shard ID from messages. Defaults to <see cref="CustomMessageExtractor"/> if not provided.</param>
        /// <returns>The same <see cref="AkkaConfigurationBuilder"/> instance for fluent chaining.</returns>
        public AkkaConfigurationBuilder AddShardRegionProxy<TMarker>(
            AkkaSettings settings,
            IMessageExtractor? messageExtractor = null)
            where TMarker : IClusterShardingSerializable
            => builder.WithShardRegionProxy<TMarker>(
                typeName: settings.Cluster.ShardRegionName,
                roleName: settings.Cluster.ShardRegionRole,
                messageExtractor: messageExtractor ?? CustomMessageExtractor.Create());
    }
}