using AetherFlow.Domain.Domains;

namespace AetherFlow.Shared.Pipeline;

public interface IAetherChunkPipelineAction
{
    AetherChunk ProcessNotification(AetherChunk chunk);
    void Sink(AetherChunk? chunk);
}