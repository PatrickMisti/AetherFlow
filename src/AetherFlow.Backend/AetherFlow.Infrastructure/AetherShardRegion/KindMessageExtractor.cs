using AetherFlow.Domain.Domains;
using AetherFlow.Shared.Messages;
using Akka.Cluster.Sharding;

namespace AetherFlow.Infrastructure.AetherShardRegion;

public sealed class KindMessageExtractor : IMessageExtractor
{
    public string? EntityId(object message) => message switch
    {
        BaseShardMessage baseMessage => baseMessage.EntityId,
        _ => throw new ArgumentException($"Unexpected message type: {message.GetType().Name}")
    };

    public object? EntityMessage(object message) => message;

    public string? ShardId(object message)
    {
        if (message is not AetherChunk chunk)
            throw new ArgumentException($"Unexpected message type: {message.GetType().Name}");

        return chunk.Kind.ToString();
    }

    public string ShardId(string entityId, object? messageHint = null) => entityId;
    
    public static KindMessageExtractor Create() => new();
}