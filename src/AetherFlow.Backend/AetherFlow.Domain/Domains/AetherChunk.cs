namespace AetherFlow.Domain.Domains;

[Serializable]
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
    public Guid Id { get; init; } = Guid.NewGuid();

    public DateTime LastWhisperUtc { get; init; } = DateTime.UtcNow;

    public bool IsValid => Kind != AetherConstructKind.Unknown &&
                           Status != AetherPresence.Unknown &&
                           Presence != ManifestationState.Unknown &&
                           ChargeState != AetherChargeState.Unknown;

    public bool IsValidValue => WarmthC is not null && 
                                WeightPressureHpa is not null && 
                                MistPercent is null &&
                                ChargePercent is null;
}