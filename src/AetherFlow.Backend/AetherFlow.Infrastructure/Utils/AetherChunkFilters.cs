using AetherFlow.Domain.Domains;

namespace AetherFlow.Infrastructure.Utils;

public static class AetherChunkFilters
{
    extension(AetherChunk c)
    {
        public bool IsUnknown() =>
            c is { Presence: ManifestationState.Unknown, ChargeState: AetherChargeState.Unknown };

        public bool IsNotUnknown() => !IsUnknown(c);

        public bool IsAlert() =>
            c.ChargeState is AetherChargeState.Critical or AetherChargeState.Fading;

        public bool IsNormal() =>
            c.ChargeState is AetherChargeState.Full
                or AetherChargeState.Stable
                or AetherChargeState.Recharging;

        public AetherChunk? FilterStale(int maxAgeMs)
        {
            var age = DateTime.UtcNow - c.LastWhisperUtc;
            return age < TimeSpan.FromMilliseconds(maxAgeMs) ? c : null;
        }
    }
}