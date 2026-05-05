namespace AetherFlow.Domain.Domains;

public enum AetherConstructKind
{
    Unknown = 0,
    EmberOrb = 1,
    MistWatcher = 2,
    ThresholdEye = 3,
    PulseAnchor = 4,
    GateCore = 5
}

public enum AetherPresence
{
    Unknown = 0,
    Dormant = 1,
    Awakened = 2,
    Slumbering = 3,
    Fractured = 4
}

public enum ManifestationState
{
    Unknown = 0,
    Absent = 1,
    Present = 2
}

public enum AetherChargeState
{
    Unknown = 0,
    Full = 1,
    Stable = 2,
    Fading = 3,
    Critical = 4,
    Recharging = 5
}