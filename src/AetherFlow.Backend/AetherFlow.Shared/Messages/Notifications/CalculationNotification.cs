using Akka.Cluster.Tools.PublishSubscribe;

namespace AetherFlow.Shared.Messages.Notifications;

public record CalculationLatencyNotification(
    string EntityId,
    TimeSpan LatencyBtwCreateAndShipped,
    TimeSpan LatencyBtwShippedAndTransformed) : INotification
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public DateTime CreatedUtc { get; init; } = DateTime.UtcNow;
}
