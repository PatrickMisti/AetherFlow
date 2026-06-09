using AetherFlow.Infrastructure.Actors;
using AetherFlow.Infrastructure.AetherDataFlow;
using AetherFlow.Ingestion.PipelineActions;
using AetherFlow.Shared.AetherInterfaces;
using AetherFlow.Shared.Messages.Ingestion;
using Akka.Actor;
using Akka.Cluster;
using Akka.Event;
using Akka.Hosting;
using Serilog;

namespace AetherFlow.Ingestion.Actors;

public sealed class AetherPipelineActor : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly IAetherPipeline _pipeline;

    private readonly string _shardRegionRole;
    private readonly IActorRef _shardProxy;
    private AetherChunkAction? _aetherChunkAction;

    private readonly IActorRef _notifyHandler;

    public AetherPipelineActor(IRequiredActor<IAetherShardProxyMarker> shardProxy,
        IRequiredActor<NotifyHandler> notifyHandler, string shardRegionRole)
    {
        _pipeline = new AetherPipeline(Log.ForContext<AetherPipelineActor>());
        _shardProxy = shardProxy.ActorRef;
        _shardRegionRole = shardRegionRole;
        _notifyHandler = notifyHandler.ActorRef;

        Receive<StartPipelineMessage>(_ => HandleStartPipeline());
        ReceiveAsync<OfferChunksMessage>(HandleOfferAsync);
        Receive<PipelineStatusRequest>(_ => HandleStatus());
        ReceiveAsync<StopPipelineMessage>(async _ =>
        {
            _log.Info("Stopping pipeline");
            await _pipeline.StopAsync();
            Context.Stop(Self);
        });

        Receive<ClusterEvent.MemberUp>(msg => HandleMemberEvent(msg.Member));
        Receive<ClusterEvent.MemberRemoved>(msg => HandleMemberEvent(msg.Member));
    }

    private void HandleStartPipeline()
    {
        _log.Info("Starting pipeline");
        _aetherChunkAction = new AetherChunkAction(log: _log, sink: _shardProxy, sender: Self, notifier: _notifyHandler);
        _pipeline.Start(_aetherChunkAction);
    }

    private Task HandleOfferAsync(OfferChunksMessage msg)
    {
        _log.Debug("Offering {ChunkCount} chunks to pipeline", msg.Chunks.Length);
        return _pipeline.OfferAsync(msg.Chunks);
    }

    private void HandleStatus() => Sender.Tell(new PipelineStatusResponse(_pipeline.IsRunning()));

    protected override void PreStart()
    {
        Cluster.Get(Context.System)
            .Subscribe(
                Self,
                typeof(ClusterEvent.MemberUp),
                typeof(ClusterEvent.MemberRemoved));
    }

    private void HandleMemberEvent(Member member)
    {
        if (!member.HasRole(_shardRegionRole)) return;

        _aetherChunkAction?.IsConnectedToShard = member.Status == MemberStatus.Up;
        _log.Info("Shard region member {MemberAddress} is now {MemberStatus}. Connected to shard: {IsConnectedToShard}",
            member.Address, member.Status, _aetherChunkAction?.IsConnectedToShard);
    }
}