using AetherFlow.Domain.EngineDomains;
using Akka.Actor;

namespace AetherFlow.Shared.Messages.Notifications;

public class MonitoringAetherChunkMessageRequest
{
    private MonitoringAetherChunkMessageRequest() { }
    
    public static MonitoringAetherChunkMessageRequest Instance() => new();
}

public record MonitoringAetherChunkMessageResponse(
    string EntityId,
    IReadOnlyList<AetherEngineValue> Values,
    IActorRef Sender) : INotification
{
    public Guid Id { get; init; } = Guid.NewGuid();
     public DateTime CreatedUtc { get; init; } = DateTime.UtcNow;
}
