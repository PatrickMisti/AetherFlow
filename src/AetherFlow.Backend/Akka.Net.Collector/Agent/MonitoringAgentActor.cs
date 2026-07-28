using Akka.Actor;
using Akka.Cluster;
using Akka.Cluster.Tools.PublishSubscribe;
using Akka.Event;
using Akka.Net.Collector.Contracts;

namespace Akka.Net.Collector.Agent;

/// <summary>
/// Runs inside the monitored system. Collects the local actor tree, subscribes to the
/// EventStream (dead letters, unhandled messages, actor telemetry) and periodically
/// publishes an <see cref="AgentSnapshot"/> to the cluster PubSub topic. It never knows
/// the collector's address — discovery happens purely through the cluster.
/// </summary>
public sealed class MonitoringAgentActor : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly IActorRef _mediator = DistributedPubSub.Get(Context.System).Mediator;
    private readonly ActorTreeCollector _tree = new(Context.System);
    private readonly List<string> _events = new();
    private int _started, _stopped, _restarted;
    private ICancelable? _tick;

    public MonitoringAgentActor()
    {
        var eventStream = Context.System.EventStream;
        eventStream.Subscribe(Self, typeof(DeadLetter));
        eventStream.Subscribe(Self, typeof(UnhandledMessage));
        eventStream.Subscribe(Self, typeof(IActorTelemetryEvent));

        Receive<DeadLetter>(d => Record($"DeadLetter: {d.Message} -> {d.Recipient}"));
        Receive<UnhandledMessage>(u => Record($"Unhandled: {u.Message} @ {u.Recipient}"));

        Receive<ActorStarted>(_ => _started++);
        Receive<ActorStopped>(_ => _stopped++);
        Receive<ActorRestarted>(_ => _restarted++);

        Receive<Flush>(_ =>
        {
            var address = Akka.Cluster.Cluster.Get(Context.System).SelfAddress.ToString();
            var snapshot = new AgentSnapshot(
                address,
                _tree.Snapshot(),
                _events.ToArray(),
                new MetricCounts(_started, _stopped, _restarted));

            _mediator.Tell(new Publish(MonitoringTopics.Snapshots, snapshot));

            _log.Info(
                "Monitoring snapshot from {0}: {1} actors, {2} events, started={3} stopped={4} restarted={5}",
                address, CountNodes(snapshot.Root), snapshot.Events.Count,
                _started, _stopped, _restarted);
        });
    }

    protected override void PreStart() =>
        _tick = Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(
            TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5), Self, Flush.Instance, Self);

    protected override void PostStop() => _tick?.Cancel();

    private static int CountNodes(ActorNode node) =>
        1 + node.Children.Sum(CountNodes);

    private void Record(string entry)
    {
        // keep the in-memory buffer bounded so the agent never leaks
        if (_events.Count >= 1000)
            _events.RemoveRange(0, _events.Count - 999);
        _events.Add(entry);
    }

    private sealed record Flush
    {
        public static readonly Flush Instance = new();
    }
}
