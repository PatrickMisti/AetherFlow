using AetherFlow.Shared.Messages.ShardRegion;
using Akka.Actor;
using Akka.Event;

namespace AetherFlow.Engine.Actors;

public class AetherEngineActor : ReceiveActor
{
    private readonly string _entityId;
    
    public  AetherEngineActor(string entityId)
    {
        _entityId = entityId;
        
        Receive<ChunkShardMessage>(msg =>
        {
            Context.GetLogger().Info($"Received message: {msg.EntityId} in actor: {_entityId}");
        });
    }
}