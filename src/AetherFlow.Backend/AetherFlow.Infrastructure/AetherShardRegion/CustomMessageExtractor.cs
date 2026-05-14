using Akka.Cluster.Sharding;

namespace AetherFlow.Infrastructure.AetherShardRegion;

public sealed class CustomMessageExtractor : HashCodeMessageExtractor
{
    private CustomMessageExtractor(int maxNumberOfShards) : base(maxNumberOfShards){}
        
    public override string? EntityId(object message) => message switch
    {
        _ => null
    };

    public static CustomMessageExtractor Create(int maxNumberOfShards = 100) => new (maxNumberOfShards);
}