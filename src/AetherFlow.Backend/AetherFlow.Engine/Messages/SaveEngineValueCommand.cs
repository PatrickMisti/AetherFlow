using AetherFlow.Domain.EngineDomains;

namespace AetherFlow.Engine.Messages;

public record SaveEngineValueCommand(AetherEngineValue EngineValue);