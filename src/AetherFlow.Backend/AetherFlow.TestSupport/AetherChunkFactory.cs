using AetherFlow.Domain.Domains;

namespace AetherFlow.TestSupport;

/// <summary>
/// Factory for building <see cref="AetherChunk"/> instances in tests, replacing the ad-hoc
/// <c>new AetherChunk(...)</c> calls that were duplicated across the actor test fixtures.
/// </summary>
public static class AetherChunkFactory
{
    /// <summary>
    /// A populated chunk with the given charge/presence state. By default it is fully populated
    /// but NOT "valid" in the domain sense (use <see cref="Valid"/> for that).
    /// </summary>
    public static AetherChunk Create(
        AetherChargeState chargeState = AetherChargeState.Stable,
        ManifestationState presence = ManifestationState.Present,
        string rune = "AETH-TEST-01",
        int chargePercent = 75) =>
        new(
            Rune: rune,
            Designation: "Test-Designation",
            Kind: AetherConstructKind.EmberOrb,
            Status: AetherPresence.Awakened,
            WarmthC: 25.5,
            WeightPressureHpa: 1013.25,
            MistPercent: 60.0,
            Presence: presence,
            ChargePercent: chargePercent,
            ChargeState: chargeState);

    /// <summary>
    /// A chunk that satisfies both <see cref="AetherChunk.IsValid"/> and <see cref="AetherChunk.IsValidValue"/>:
    /// known kind/status/presence/charge-state and warmth + pressure set with mist + charge-percent null.
    /// </summary>
    public static AetherChunk Valid(
        AetherChargeState chargeState = AetherChargeState.Stable,
        ManifestationState presence = ManifestationState.Present,
        string rune = "AETH-TEST-01") =>
        new(
            Rune: rune,
            Designation: "Test-Designation",
            Kind: AetherConstructKind.EmberOrb,
            Status: AetherPresence.Awakened,
            WarmthC: 25.5,
            WeightPressureHpa: 1013.25,
            MistPercent: null,
            Presence: presence,
            ChargePercent: null,
            ChargeState: chargeState);
}
