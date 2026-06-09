using AetherFlow.Domain.Domains;
using AetherFlow.Shared.Messages.Notifications;
using AetherFlow.Shared.Messages.ShardRegion;
using AetherFlow.Shared.Pipeline;
using Akka.Actor;
using Akka.Event;

namespace AetherFlow.Ingestion.PipelineActions;

internal class AetherChunkAction(ILoggingAdapter log, IActorRef sink, IActorRef sender, IActorRef notifier) : IAetherChunkPipelineAction
{
    public bool IsConnectedToShard { get; set; }

    public AetherChunk ProcessNotification(AetherChunk chunk)
    {
        log.Debug("Processing notification for chunk: {ChunkId}", chunk.Id);
        
        if (!chunk.IsValid || !chunk.IsValidValue)
        {
            log.Debug("Chunk is not valid UnkownType: {UnknownType} NoValue: {NoValue}", !chunk.IsValid, !chunk.IsValidValue);
            notifier.Tell(new ChunkAnomalyNotification(
                Designation: chunk.Designation,
                NoValue: !chunk.IsValidValue,
                UnknownType: !chunk.IsValid));
        }
        
        return chunk;
    }

    public void Sink(AetherChunk? chunk)
    {
        log.Debug("Sinking chunk: {ChunkId} to storage", chunk?.Id);
        if (chunk is null) return;
        
        if (IsConnectedToShard)
            sink.Tell(new ChunkShardMessage(
                EntityId: chunk.Rune,
                Chunk: chunk,
                Sender: sender));
    }
}