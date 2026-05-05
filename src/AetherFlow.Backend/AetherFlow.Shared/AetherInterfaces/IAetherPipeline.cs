using AetherFlow.Domain.Domains;
using AetherFlow.Infrastructure.AetherDataFlow;
using AetherFlow.Shared.Pipeline;

namespace AetherFlow.Shared.AetherInterfaces;

public interface IAetherPipeline
{
    void Start(IAetherChunkPipelineAction action, AetherPipelineOptions? options = null);
    void Stop();
    
    Task OfferAsync(params AetherChunk[] chunks);

    bool IsRunning();
    
}