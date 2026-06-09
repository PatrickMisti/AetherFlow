using AetherFlow.Domain.Domains;

namespace AetherFlow.Domain.EngineDomains;

public record struct AetherEngineValue(
    Guid Id,
    string Designation,
    double WarmthC,
    double Hpa,
    double MistPercent,
    int Charging,
    AetherPresence Status,
    DateTime Date);

public static class AetherEngineValueExtensions
{
    public static AetherEngineValue ToEngineValue(this AetherChunk chunk) => new(
        Id: chunk.Id,
        Designation: chunk.Designation,
        WarmthC: chunk.WarmthC ?? 0,
        Hpa: chunk.WeightPressureHpa ?? 0,
        MistPercent: chunk.MistPercent ?? 0,
        Charging: chunk.ChargePercent ?? 0,
        Status: chunk.Status,
        Date: DateTime.UtcNow
    );
}