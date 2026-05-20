using System.Net;

namespace AetherFlow.Shared.Config;

public class AkkaSettings
{
    public RemoteSettings Remote { get; set; } = new();
    public ClusterSettings Cluster { get; set; } = new();
    public ManagementSettings Management { get; set; } = new();

    public class RemoteSettings
    {
        public string Host { get; set; } = Dns.GetHostName();
        public int Port { get; set; } = 8081;
    }

    public class ClusterSettings
    {
        public string[] Roles { get; set; } = [];
        public string ServiceName { get; set; } = "aether-cluster";
        public int RequiredContactPoints { get; set; } = 1;
    }

    public class ManagementSettings
    {
        public int Port { get; set; } = 8888;
    }
}