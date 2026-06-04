using AetherFlow.Domain.Domains;
using AetherFlow.Shared.Messages.ShardRegion;
using AetherFlow.Shared.Pipeline;
using Akka.Actor;
using Akka.Event;

namespace AetherFlow.Ingestion.PipelineActions;

internal class AetherChunkAction(ILoggingAdapter log, IActorRef sink, IActorRef sender) : IAetherChunkPipelineAction
{
    public bool IsConnectedToShard { get; set; }

    public AetherChunk ProcessNotification(AetherChunk chunk)
    {
        log.Debug("Processing notification for chunk: {ChunkId}", chunk.Id);
        // Todo: send anomaly detection events to live_view
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