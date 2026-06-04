using Akka.Actor;

namespace AetherFlow.Shared.Messages;

public record BaseShardMessage(string EntityId, IActorRef Sender)
{
    public DateTime Created { get; init; } = DateTime.Now;
}
    