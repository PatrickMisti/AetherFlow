using AetherFlow.Domain.Domains;
using AetherFlow.Infrastructure.Actors;
using AetherFlow.Shared.Messages.Notifications;
using AetherFlow.SystemTests;
using Akka.Actor;
using Akka.Cluster;
using Akka.Configuration;
using Akka.Discovery;
using Akka.Routing;

Console.WriteLine("Hello, AetherFlow System Tests!");

async Task DiscoveryTest()
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
    await actorSystem.WhenTerminated;
}

async Task ClusterTesting()
{
    var actorSystem = ActorSystem.Create("AetherFlowCluster", HoconConfig.GetShardConfig());
    var cluster = Cluster.Get(actorSystem);
    // Warten bis Cluster bereit
    await cluster.JoinAsync(new Address("akka.tcp", "AetherFlowCluster", "localhost", 8091));
    actorSystem.ActorOf(ShardMonitorActor.Props(), "shardMonitor");
    await actorSystem.WhenTerminated;
}

async Task NotificationHandlerTesting()
{
    var actorSystem = ActorSystem.Create("AetherFlowActor", ConfigurationFactory.Default());
    var notify = actorSystem.ActorOf(Props.Create(() => new NotifyHandler()), "notifyHandler");
    foreach (var i in Enumerable.Range(1, 200))
    {
        notify.Tell(new ChargingLevelNotification($"Entity{i}", AetherChargeState.Full));
    }
    await actorSystem.WhenTerminated;
}

async Task NotificationHandlerTesting1()
{
    var actorSystem = ActorSystem.Create("AetherFlowActor", ConfigurationFactory.Default());

    var resizer = new DefaultResizer(lower: 2, upper: 16, messagesPerResize: 1, rampupRate: .5);
    var notify = actorSystem.ActorOf(
        Props.Create(() => new NotifyHandler())
            .WithRouter(new RoundRobinPool(2, resizer)),
        "notifyHandler");

    foreach (var i in Enumerable.Range(1, 200))
    {
        
        notify.Tell(new ChargingLevelNotification($"Entity{i}", AetherChargeState.Full));
        await Task.Delay(100); // needed without not work router think it's ok with burst 
    }
    
    for (int k = 0; k < 50; k++)
    {
        var r = await notify.Ask<Routees>(GetRoutees.Instance, TimeSpan.FromSeconds(1));
        Console.WriteLine($"t={k * 200}ms  routees={r.Members.Count()}");
        await Task.Delay(200);
    } 
    await actorSystem.Terminate();
}
await NotificationHandlerTesting1();