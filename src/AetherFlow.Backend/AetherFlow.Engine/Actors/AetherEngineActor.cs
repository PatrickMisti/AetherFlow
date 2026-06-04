using AetherFlow.Shared.Messages.ShardRegion;
using Akka.Event;
using Akka.Persistence;

namespace AetherFlow.Engine.Actors;

public class AetherEngineActor : ReceivePersistentActor
{
    public override string PersistenceId { get; } = Context.Self.Path.Name;
    private readonly ILoggingAdapter _log = Context.GetLogger();

    public AetherEngineActor()
    {
        // Todo: handle chunk messages and perform anomaly detection
        // Todo: persist chunk messages and replay on recovery
        // Todo: make notifier for live view updates and send notifications on anomalies
        // setup should work now with akka.db 
        Command<ChunkShardMessage>(HandleChunkMessage);
    }

    private void HandleChunkMessage(ChunkShardMessage msg)
    {
        _log.Debug("Handling chunk message for entity {EntityId}", msg.EntityId);
    }
}