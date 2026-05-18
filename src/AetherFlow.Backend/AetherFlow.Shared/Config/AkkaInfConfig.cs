namespace AetherFlow.Shared.Config;

public record struct AkkaInfConfig(string Host, int Port, string[] Roles, string[] SeedNodes, string ActiveRole);