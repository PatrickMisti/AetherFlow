using AetherFlow.Domain.Domains;
using AetherFlow.Shared.Pipeline;

namespace AetherFlow.Shared.AetherInterfaces;

public interface IAetherPipeline
{
    void Start(IAetherChunkPipelineAction action, AetherPipelineOptions? options = null);
    Task StopAsync();
    
    Task OfferAsync(params AetherChunk[] chunks);

    bool IsRunning();
    
}