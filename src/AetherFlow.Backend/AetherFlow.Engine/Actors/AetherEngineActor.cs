using AetherFlow.Domain.Domains;
using AetherFlow.Shared.Messages.ShardRegion;
using Akka.Event;
using Akka.Persistence;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace AetherFlow.Engine.Actors;

public class AetherEngineActor : ReceivePersistentActor
{
    public override string PersistenceId { get; } = Context.Self.Path.Name;
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private ISourceQueueWithComplete<AetherChunk>? _sourceQueue;
    private readonly SharedKillSwitch _killSwitch;

    public AetherEngineActor(SharedKillSwitch? killSwitch = null)
    {
        _killSwitch = killSwitch ?? KillSwitches.Shared("aether-engine-kill-switch");
        // Todo: handle chunk messages and perform anomaly detection
        // Todo: persist chunk messages and replay on recovery
        // Todo: make notifier for live view updates and send notifications on anomalies
        // setup should work now with akka.db 
        Command<ChunkShardMessage>(msg =>
        {
            _log.Debug("Got command {entityId} with send msg time: {created} time now: {now}",
                msg.EntityId,
                msg.Created,
                DateTime.UtcNow);

            _sourceQueue?.OfferAsync(msg.Chunk);
            SaveSnapshot(msg);
        });
    }
    
    protected override void PreStart()
    {
        base.PreStart();
        _sourceQueue = Source.Queue<AetherChunk>(bufferSize: 100, overflowStrategy: OverflowStrategy.DropHead)
            .Via(_killSwitch.Flow<AetherChunk>())
            .Via(Flow.Create<AetherChunk>().Select(msg =>
            {
                Console.WriteLine("processing.....");
                return msg;
            }))
            .To(Sink.Ignore<AetherChunk>())
            .Run(Context.System.Materializer());
    }

    protected override void PostStop()
    {
        _sourceQueue?.Complete();
        _killSwitch.Shutdown();
        base.PostStop();
    }
}