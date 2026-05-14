using AetherFlow.Shared.AetherInterfaces;
using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Hosting;

namespace AetherFlow.Engine;

public class Worker(IRequiredActor<IAetherShardMarker> markerRef) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            // Simulate work by delaying for a short period of time
            await Task.Delay(1000, stoppingToken);
            markerRef.ActorRef.Tell(new ShardingEnvelope(entityId: "test-entity", message: new Test("Hello from Worker!")));
        }
    }
}

public record Test(string Message);
