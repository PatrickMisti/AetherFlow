using AetherFlow.SystemTests;
using Akka.Actor;
using Akka.Cluster;
using Akka.Discovery;

Console.WriteLine("Hello, AetherFlow System Tests!");

async Task discoveryTest() 
{
    var actorSystem = ActorSystem.Create("AetherFlowSystemTests", HoconConfig.GetConfig());
    var discovery = Discovery.Get(actorSystem);

    var result = discovery.Default.Lookup(
        new Lookup(
            serviceName: "aether-engine",
            portName: null,
            protocol: null),
        resolveTimeout: TimeSpan.FromSeconds(5)).Result;

    Console.WriteLine("Result is ----------------------");
    foreach (var resolved in result.Addresses)
    {
        Console.WriteLine($"Host: {resolved.Host}");
        Console.WriteLine($"Port: {resolved.Port}");
        Console.WriteLine($"Address: {resolved.Address}");
    }

    Console.WriteLine("Joining cluster...");
    var cluster = Cluster.Get(actorSystem);

    await cluster.JoinAsync(cluster.SelfAddress);

    cluster.RegisterOnMemberUp(() => { Console.WriteLine("Cluster member is up!"); });
}

var actorSystem = ActorSystem.Create("AetherFlowCluster", HoconConfig.GetShardConfig());

// Warten bis Cluster bereit
var cluster = Cluster.Get(actorSystem);
await cluster.JoinAsync(new Address("akka.tcp", "AetherFlowCluster", "localhost", 8091));

actorSystem.ActorOf(ShardMonitorActor.Props(), "shardMonitor");

await actorSystem.WhenTerminated;