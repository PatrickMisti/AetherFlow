using System.Net;
using Akka.Cluster.Hosting;
using Akka.Management;
using Akka.Remote.Hosting;

namespace AetherFlow.Shared.Config;

public class AkkaSettings
{
    public string ActorSystemName { get; set; } = "DrawTogether";

    public bool LogConfigOnStart { get; set; } = false;

    public RemoteOptions RemoteOptions { get; set; } = new()
    {
        // can be overridden via config, but is dynamic by default
        PublicHostName = Dns.GetHostName(),
        Port = 8081
    };

    public ClusterOptions ClusterOptions { get; set; } = new ClusterOptions()
    {
        // use our dynamic local host name by default
        SeedNodes = [$"akka.tcp://DrawTogether@{Dns.GetHostName()}:8081"],
        Roles = ["aether-engine"]
    };

    public ShardOptions ShardOptions { get; set; } = new ShardOptions();
    
    public AkkaManagementOptions? AkkaManagementOptions { get; set; }
}