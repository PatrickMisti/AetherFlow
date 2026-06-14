using System.Net;

namespace AetherFlow.Shared.Config;

/// <summary>
/// Root configuration object for Akka.NET networking, clustering, and management endpoints.
/// This model is typically bound from configuration files and used during actor system startup.
/// </summary>
public class AkkaSettings
{
    /// <summary>
    /// Remote transport configuration used by Akka.Remote.
    /// </summary>
    public RemoteSettings Remote { get; set; } = new();

    /// <summary>
    /// Cluster behavior and membership configuration used by Akka.Cluster.
    /// </summary>
    public ClusterSettings Cluster { get; set; } = new();

    /// <summary>
    /// HTTP management endpoint configuration used by Akka.Management.
    /// </summary>
    public ManagementSettings Management { get; set; } = new();

    public RouterResizer Resizer { get; set; } = new();

    /// <summary>
    /// Configures Akka.Remote transport settings.
    /// Needed so this node can accept remote actor traffic on a known host and port.
    /// The port should be unique per node when multiple nodes run on the same machine; when nodes run on different hosts
    /// the port can be the same because the host IP differentiates endpoints.
    /// </summary>
    public class RemoteSettings
    {
        /// <summary>
        /// Hostname or IP address this node binds/advertises for remote actor communication.
        /// </summary>
        public string Host { get; set; } = Dns.GetHostName();

        /// <summary>
        /// TCP port used for Akka.Remote traffic.
        /// </summary>
        public int Port { get; set; } = 8091;
    }

    /// <summary>
    /// Configures Akka.Cluster participation options.
    /// Needed to assign node roles, identify the cluster service, and define minimum seed/contact requirements.
    /// Roles define what responsibilities a node can take and are used for role-based deployment and routing.
    /// ServiceName must be the same for nodes in the same cluster and different across separate clusters so discovery can distinguish them.
    /// </summary>
    public class ClusterSettings
    {
        /// <summary>
        /// Logical roles assigned to this node (for role-based deployment and routing).
        /// </summary>
        public string[] Roles { get; set; } = Array.Empty<string>();

        /// <summary>
        /// Cluster service/discovery name shared by nodes that belong to the same cluster.
        /// </summary>
        public string ServiceName { get; set; } = "aether-cluster";

        /// <summary>
        /// Minimum number of discovered contact points required before bootstrap proceeds.
        /// </summary>
        public int RequiredContactPoints { get; set; } = 1;

        /// <summary>
        /// Minimum number of members required before the cluster transitions to <c>Up</c>.
        /// This setting does not prevent local actors from starting before the cluster is <c>Up</c>.
        /// Currently not wired up — <c>AddClustering</c> does not pass this to the cluster options.
        /// </summary>
        public int MinimumNumberOfMembers { get; set; } = 1;

        /// <summary>
        /// Optional static seed nodes used for cluster formation. Each entry should be a full Akka remoting address,
        /// e.g. "akka.tcp://AetherFlowCluster@hostname:8091". When using dynamic discovery (DNS/Kubernetes), prefer
        /// leaving this empty and relying on discovery instead.
        /// </summary>
        public string[] SeedNodes { get; set; } = Array.Empty<string>();

        /// <summary>
        /// Logical name used to identify the shard region in the cluster.
        /// Must be consistent across all nodes that participate in the same shard region.
        /// </summary>
        public string ShardRegionName { get; set; } = "ShardRegion";

        /// <summary>
        /// Cluster role required to host shards for this region.
        /// Must match one of the roles in <see cref="Roles"/> on every node intended to host shards.
        /// Defaults to <c>aether-engine</c>. If a null role were passed at registration, the shard
        /// region would be hosted on every cluster node.
        /// </summary>
        public string ShardRegionRole { get; set; } = "aether-engine";

        /// <summary>
        /// Split-brain resolver strategy used when a network partition occurs.
        /// Defaults to keep-majority. Currently not wired up — <c>AddClustering</c> disables SBR;
        /// configure it via HOCON or apply this setting in <c>AddClustering</c> before relying on it.
        /// </summary>
        public SplitBrainResolverSettings SplitBrainResolver { get; set; } = new();

        /// <summary>
        /// Configuration for Akka split-brain handling.
        /// </summary>
        public class SplitBrainResolverSettings
        {
            /// <summary>
            /// Strategy name: <c>keep-majority</c>, <c>static-quorum</c>, or <c>keep-oldest</c>.
            /// </summary>
            public string Strategy { get; set; } = "keep-majority";

            /// <summary>
            /// Quorum size used when <see cref="Strategy"/> is <c>static-quorum</c>.
            /// </summary>
            public int QuorumSize { get; set; } = 1;

            /// <summary>
            /// Whether the oldest node should down itself when it becomes isolated.
            /// Only used when <see cref="Strategy"/> is <c>keep-oldest</c>.
            /// </summary>
            public bool DownIfAlone { get; set; }
        }
    }

    /// <summary>
    /// Configures Akka.Management HTTP endpoint settings.
    /// Used for cluster bootstrap, health checks and operational discovery.
    /// When running multiple processes on one host the management port must be unique per process; when running on
    /// separate hosts the same port is fine.
    /// </summary>
    public class ManagementSettings
    {
        /// <summary>
        /// HTTP port exposed by Akka.Management for bootstrap and operational endpoints.
        /// </summary>
        public int Port { get; set; } = 8888;
    }

    public class RouterResizer
    {
        public int Low { get; set; } = 2;
        public int High { get; set; } = 15;
    }
}