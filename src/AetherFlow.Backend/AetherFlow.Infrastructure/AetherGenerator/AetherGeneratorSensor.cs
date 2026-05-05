using AetherFlow.Domain.Domains;
using AetherFlow.Shared.AetherInterfaces;

namespace AetherFlow.Infrastructure.AetherGenerator;

public class AetherGeneratorSensor : IPeripheryConnector<AetherChunk>
{
    private Random? _random;

    private static readonly string[] Rune =
    [
        "AETH-ZEPHYR-01", "AETH-NIMBUS-02", "AETH-VORTEX-03",
        "AETH-STRATOS-04", "AETH-LUMINAL-05", "AETH-OBSIDIAN-06",
        "AETH-AURORA-07",  "AETH-FRACTAL-08","AETH-SOLARIS-09",
        "AETH-DRIFT-10",
    ];

    public void Connect()
    {
        _random = new Random();
    }

    public AetherChunk GenerateData()
    {
        ArgumentNullException.ThrowIfNull(_random);

        var rune = Rune[_random.Next(Rune.Length)];
        var kind = (AetherConstructKind)_random.Next(1, Enum.GetValues(typeof(AetherConstructKind)).Length);
        var status = (AetherPresence)_random.Next(1, Enum.GetValues(typeof(AetherPresence)).Length);
        var presence = (ManifestationState)_random.Next(1, Enum.GetValues(typeof(ManifestationState)).Length);
        var chargeState = (AetherChargeState)_random.Next(1, Enum.GetValues(typeof(AetherChargeState)).Length);

        return new AetherChunk(
            Rune: rune,
            Designation: $"{rune}-{Guid.NewGuid():N}".Substring(0, 36),
            Kind: kind,
            Status: status,
            WarmthC: _random.Next(-50, 50) + _random.NextDouble(),
            WeightPressureHpa: 900 + _random.Next(0, 200) + _random.NextDouble(),
            MistPercent: _random.Next(0, 101) + _random.NextDouble(),
            Presence: presence,
            ChargePercent: _random.Next(0, 101),
            ChargeState: chargeState
        );
    }

    public void Disconnect()
    {
        _random = null;
    }
}