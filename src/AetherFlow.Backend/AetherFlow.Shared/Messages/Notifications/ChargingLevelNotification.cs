using AetherFlow.Domain.Domains;

namespace AetherFlow.Shared.Messages.Notifications;

public record ChargingLevelNotification(string EntityId, AetherChargeState Level): INotification
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public DateTime CreatedUtc { get; init; } = DateTime.UtcNow;
};