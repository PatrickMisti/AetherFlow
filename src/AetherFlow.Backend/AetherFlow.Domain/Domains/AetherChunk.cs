namespace AetherFlow.Domain.Domains;

public record AetherChunk(
    string Rune = "",
    string Designation = "",
    AetherConstructKind Kind = AetherConstructKind.Unknown,
    AetherPresence Status = AetherPresence.Dormant,
    double? WarmthC = null,
    double? WeightPressureHpa = null,
    double? MistPercent = null,
    ManifestationState Presence = ManifestationState.Unknown,
    int? ChargePercent = null,
    AetherChargeState ChargeState = AetherChargeState.Unknown)
{
    public Guid Id { get; } = Guid.NewGuid();

    public DateTime LastWhisperUtc { get; init; } = DateTime.UtcNow;
}