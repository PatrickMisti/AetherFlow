using Akka.Actor;

namespace AetherFlow.Shared.Messages;

public record BaseShardMessage(string EntityId, IActorRef ActorRef)
{
    public DateTime Created { get; init; } = DateTime.Now;
}
    