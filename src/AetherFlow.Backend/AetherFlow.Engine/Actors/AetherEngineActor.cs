using AetherFlow.Shared.Messages.ShardRegion;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence;

namespace AetherFlow.Engine.Actors;

public class AetherEngineActor : ReceivePersistentActor
{
    public override string PersistenceId { get; } = Context.Self.Path.Name;
    private readonly ILoggingAdapter _log = Context.GetLogger();

    public AetherEngineActor()
    {
        Command<ChunkShardMessage>(HandleChunkMessage);
    }

    private void HandleChunkMessage(ChunkShardMessage msg)
    {
        _log.Debug("Handling chunk message for entity {EntityId}", msg.EntityId);
    }
}