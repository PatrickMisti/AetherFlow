using AetherFlow.Domain.Domains;

namespace AetherFlow.Shared.Messages.Ingestion;

public record StartPipelineMessage
{
    public static StartPipelineMessage Instance => new ();
}

public record StopPipelineMessage
{
    public static StopPipelineMessage Instance => new ();
}

public record OfferChunksMessage(params AetherChunk[] Chunks);

public record PipelineStatusRequest
{
    public static PipelineStatusRequest Instance => new ();
};
public record PipelineStatusResponse(bool IsRunning);