using AetherFlow.Domain.Domains;
using AetherFlow.Domain.EngineDomains;
using AetherFlow.Shared.Messages.Notifications;
using AetherFlow.Shared.Messages.ShardRegion;
using Akka.Actor;
using Akka.Cluster.Tools.PublishSubscribe;
using Akka.Event;
using Akka.Persistence;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace AetherFlow.Engine.Actors;

public class AetherEngineActor : ReceivePersistentActor
{
    public override string PersistenceId { get; } = Context.Self.Path.Name;
    private readonly ILoggingAdapter _log = Context.GetLogger();

    private ISourceQueueWithComplete<ChunkShardMessage>? _sourceQueue;
    private readonly SharedKillSwitch _killSwitch;

    private int _capacity = 100;
    private readonly Queue<AetherEngineValue> _items = new();
    private IReadOnlyCollection<AetherEngineValue> Items => _items.ToList().AsReadOnly();

    public AetherEngineActor(SharedKillSwitch? killSwitch = null)
    {
        _killSwitch = killSwitch ?? KillSwitches.Shared("aether-engine-kill-switch");
        Command<SubscribeAck>(_ => Become(Initialize));
    }

    private void Initialize()
    {
        Command<ChunkShardMessage>(msg =>
        {
            _log.Debug("Got command {entityId} with send msg time: {created} time now: {now}",
                msg.EntityId,
                msg.Created,
                DateTime.UtcNow);

            _sourceQueue?.OfferAsync(msg);
        });

        Command<ChunkCapacityChangeNotification>(msg =>
        {
            _log.Debug("Capacity change command received: {capacity}", msg.Capacity);
            _capacity = msg.Capacity;

            SaveSnapshot(msg);
        });

        RecoveryMessages();
    }

    private void RecoveryMessages()
    {
        Recover<AetherEngineValue>(msg =>
        {
            if (_items.Count >= _capacity) _items.Dequeue();
            _items.Enqueue(msg);
        });

        Recover<SnapshotOffer>(snapshot =>
        {
            switch (snapshot.Snapshot)
            {
                case ChunkCapacityChangeNotification cap:
                    _log.Debug("Recovered capacity change: {capacity}", cap.Capacity);
                    _capacity = cap.Capacity;
                    break;
                case AetherEngineValue msg:
                    _log.Debug("Recovered chunk shard message for entity {entityId} with created time: {created}",
                        msg.Designation, msg.Date);
                    _items.Enqueue(msg);
                    break;
                default:
                    _log.Warning("Unknown snapshot type: {snapshotType}", snapshot.Snapshot.GetType().Name);
                    break;
            }
        });
    }


    private AetherEngineValue TransformAndNotifyHandler(AetherChunk chunk)
    {
        _log.Debug("Transform chunk to engine value");
        if (chunk.Presence == ManifestationState.Absent) 
        {
            // todo send to handler
            _log.Debug("Chunk is absent, sending manifestation state absent notification");
            // ManifestationStateAbsentNotification.Instance
        }
        
        _log.Debug("Chunk charging level is {chargeState}, sending charge state notification", chunk.ChargeState);
        // todo send to handler
        // ChargingLevelNotification
        
        return chunk.ToEngineValue();
    }

    private AetherChunk UnpackMsgAndNotifyHandler(ChunkShardMessage msg)
    {
        _log.Debug("Processing unpack chunk shard message for entity {entityId} with created time: {created}", msg.EntityId,
            msg.Created);
        
        var notification = new CalculationLatencyNotification(
            msg.EntityId,
            LatencyBtwCreateAndShipped: msg.Created - msg.Chunk.LastWhisperUtc,
            LatencyBtwShippedAndTransformed: DateTime.UtcNow - msg.Created
        );
        
        // todo send to handler
        return msg.Chunk;
    }

    private void SaveEngineValue(AetherEngineValue engineValue) => Persist(engineValue, value =>
    {
        if (_items.Count >= _capacity) _items.Dequeue();
        _items.Enqueue(value);
    });

    protected override void PreStart()
    {
        base.PreStart();

        var mediator = DistributedPubSub.Get(Context.System).Mediator;
        mediator.Tell(new Subscribe("capacity", Self));

        _sourceQueue = Source.Queue<ChunkShardMessage>(bufferSize: 100, overflowStrategy: OverflowStrategy.DropHead)
            .Via(_killSwitch.Flow<ChunkShardMessage>())
            .Via(Flow.Create<ChunkShardMessage>().Select(UnpackMsgAndNotifyHandler))
            .Via(Flow.Create<AetherChunk>().Select(TransformAndNotifyHandler))
            .To(Sink.ForEach<AetherEngineValue>(SaveEngineValue))
            .Run(Context.System.Materializer());
    }

    protected override void PostStop()
    {
        _sourceQueue?.Complete();
        _killSwitch.Shutdown();
        base.PostStop();
    }
}