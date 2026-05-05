using AetherFlow.Domain.Domains;

namespace AetherFlow.Shared.Messages.Ingestion;


public record DispatchWork;
public record StartGenerator;

public record GenerateChunk(int ReadingsPerChunk, int Index);

public record StopWorker;

public record GeneratedChunk(int Index, ICollection<AetherChunk> Chunks);