using Akka.Actor;
using Akka.Net.Collector.Contracts;

namespace Akka.Net.Collector.Agent;

/// <summary>
/// Walks the local actor hierarchy via the root guardian. This only works in-process,
/// which is exactly why the agent lives inside the monitored ActorSystem.
/// Uses internal Akka APIs (<see cref="IInternalActorRef.Children"/>) — may break on Akka upgrades.
/// </summary>
public sealed class ActorTreeCollector(ActorSystem system)
{
    public ActorNode Snapshot()
    {
        IActorRef root = ((ExtendedActorSystem)system).Provider.RootGuardian;
        return Build(root);
    }

    private static ActorNode Build(IActorRef actor)
    {
        var children = actor is ActorRefWithCell withCell
            ? withCell.Children.Select(Build).ToList()
            : new List<ActorNode>();

        return new ActorNode(actor.Path.ToStringWithoutAddress(), actor.Path.Name, children);
    }
}
