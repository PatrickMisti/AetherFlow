using AetherFlow.Infrastructure.AetherShardRegion;
using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Event;

namespace AetherFlow.SystemTests;
public class ShardMonitorActor : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();

    public ShardMonitorActor()
    {
        Receive<ClusterShardingStats>(stats =>
        {
            _log.Info("=== Cluster Shard State ===");
            foreach (var (address, regionStats) in stats.Regions)
            {
                _log.Info("Node: {address}", address);
                foreach (var (shardId, entityCount) in regionStats.Stats)
                    _log.Info("  Shard [{shardId}] → {count} entities", shardId, entityCount);
            }
        });
    }

    protected override void PreStart()
    {
        base.PreStart();
        
        var sharding = ClusterSharding.Get(Context.System);
        var proxy = sharding.StartProxy(
            typeName: "aether-shard",
            role: "aether-engine",
            messageExtractor: KindMessageExtractor.Create()
        );

        Context.System.Scheduler.ScheduleTellRepeatedly(
            TimeSpan.FromSeconds(5),
            TimeSpan.FromSeconds(5),
            proxy,
            new GetClusterShardingStats(TimeSpan.FromSeconds(3)),
            Self
        );
    }

    public static Props Props() => Akka.Actor.Props.Create<ShardMonitorActor>();
}