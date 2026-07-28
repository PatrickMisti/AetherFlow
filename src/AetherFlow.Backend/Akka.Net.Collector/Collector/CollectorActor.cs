using Akka.Actor;
using Akka.Cluster.Tools.PublishSubscribe;
using Akka.Net.Collector.Contracts;

namespace Akka.Net.Collector.Collector;

/// <summary>
/// Runs in the collector app. Subscribes to the PubSub topic and keeps the latest snapshot
/// per node. Answers <see cref="GetAll"/> queries from the HTTP API.
/// </summary>
public sealed class CollectorActor : ReceiveActor
{
    private readonly Dictionary<string, AgentSnapshot> _byNode = new();

    public CollectorActor()
    {
        var mediator = DistributedPubSub.Get(Context.System).Mediator;
        mediator.Tell(new Subscribe(MonitoringTopics.Snapshots, Self));

        Receive<AgentSnapshot>(s => _byNode[s.NodeAddress] = s);
        Receive<SubscribeAck>(_ => { /* subscription confirmed */ });
        Receive<GetAll>(_ => Sender.Tell(_byNode.Values.ToArray()));
    }

    /// <summary>Query for every node's most recent snapshot. Replies with <c>AgentSnapshot[]</c>.</summary>
    public sealed record GetAll
    {
        public static readonly GetAll Instance = new();
    }
}
