using AetherFlow.Infrastructure.AetherDataFlow;
using AetherFlow.Ingestion.PipelineActions;
using AetherFlow.Shared.AetherInterfaces;
using AetherFlow.Shared.Messages.Ingestion;
using Akka.Actor;
using Akka.Event;
using Serilog;

namespace AetherFlow.Ingestion.Actors;

public sealed class AetherPipelineActor : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly IAetherPipeline _pipeline;

    public AetherPipelineActor()
    {
        _pipeline = new AetherPipeline(Log.ForContext<AetherPipelineActor>());

        Receive<StartPipelineMessage>(_ => HandleStartPipeline());
        ReceiveAsync<OfferChunksMessage>(HandleOfferAsync);
        Receive<PipelineStatusRequest>(_ => HandleStatus());
        ReceiveAsync<StopPipelineMessage>(async _ =>
        {
            _log.Info("Stopping pipeline");
            await _pipeline.StopAsync();
            Context.Stop(Self);
        });
    }

    private void HandleStartPipeline()
    {
        _log.Info("Starting pipeline");
        var action = new AetherChunkAction(_log);
        _pipeline.Start(action);
    }

    private async Task HandleOfferAsync(OfferChunksMessage msg)
    {
        _log.Debug("Offering {ChunkCount} chunks to pipeline", msg.Chunks.Length);
        await _pipeline.OfferAsync(msg.Chunks);
    }

    private void HandleStatus() => Sender.Tell(new PipelineStatusResponse(_pipeline.IsRunning()));
}