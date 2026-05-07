using AetherFlow.Domain.Domains;
using AetherFlow.Shared.Pipeline;
using Akka.Event;

namespace AetherFlow.Ingestion.PipelineActions;

internal class AetherChunkAction(ILoggingAdapter log) : IAetherChunkPipelineAction
{
    public AetherChunk ProcessNotification(AetherChunk chunk)
    {
        log.Info("Processing notification for chunk: {ChunkId}", chunk.Id);
        // Todo: send anomaly detection events to live_view
        return chunk;
    }

    public void Sink(AetherChunk? chunk)
    {
        log.Info("Sinking chunk: {ChunkId} to storage", chunk?.Id);
        // Todo: implement sink -> Akka Shard Region
    }
}