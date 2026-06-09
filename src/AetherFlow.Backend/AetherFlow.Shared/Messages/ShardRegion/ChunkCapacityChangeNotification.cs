using AetherFlow.Shared.Messages.Notifications;
using Akka.Actor;

namespace AetherFlow.Shared.Messages.ShardRegion;

public record ChunkCapacityChangeNotification(int Capacity, IActorRef Sender) : INotification
{
    public Guid Id { get; } = Guid.NewGuid();
}