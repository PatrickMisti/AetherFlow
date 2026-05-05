using AetherFlow.Domain.Domains;
using AetherFlow.Shared.Pipeline;
using ILogger = Serilog.ILogger;

namespace AetherFlow.Ingestion.PipelineActions;

internal class AetherChunkAction(ILogger log) : IAetherChunkPipelineAction
{
    public AetherChunk ProcessNotification(AetherChunk chunk)
    {
        log.Information("Processing notification for chunk: {ChunkId}", chunk.Id);
        // Todo: send anomaly detection events to live_view
        return chunk;
    }

    public void Sink(AetherChunk? chunk)
    {
        log.Information("Sinking chunk: {ChunkId} to storage", chunk?.Id);
        // Todo: implement sink -> Akka Shard Region
    }
}