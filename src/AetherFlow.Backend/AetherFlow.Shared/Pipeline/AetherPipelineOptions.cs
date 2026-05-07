namespace AetherFlow.Shared.Pipeline;

public sealed class AetherPipelineOptions
{
    public int AlertStaleMs { get; init; } = 500;
    public int NormalStaleMs { get; init; } = 2000;
    public int BoundedCapacity { get; init; } = 100;
}