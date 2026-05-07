using AetherFlow.Domain.Domains;
using AetherFlow.Shared.AetherInterfaces;
using AetherFlow.Shared.Pipeline;
using System.Threading.Tasks.Dataflow;
using AetherFlow.Infrastructure.Utils;
using Akka.Event;

namespace AetherFlow.Infrastructure.AetherDataFlow;

public class AetherPipeline(ILoggingAdapter logger) : IAetherPipeline
{
    private BufferBlock<AetherChunk>? _pipeline;
    private bool _isRunning;

    private Task _completion = Task.CompletedTask;

    public void Start(IAetherChunkPipelineAction action, AetherPipelineOptions? options)
    {
        lock (this)
        {
            if (IsRunning()) return;

            try
            {
                var opts = options ?? new AetherPipelineOptions();
                var linkOpts = new DataflowLinkOptions { PropagateCompletion = true };
                var blockOpts = new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = opts.BoundedCapacity
                };

                logger.Info(
                    "AetherPipeline starting — Capacity={Capacity}, AlertStaleMs={AlertMs}, NormalStaleMs={NormalMs}",
                    opts.BoundedCapacity, opts.AlertStaleMs, opts.NormalStaleMs);

                _pipeline = new BufferBlock<AetherChunk>(blockOpts);

                var alertBlock = new TransformBlock<AetherChunk, AetherChunk>(chunk =>
                {
                    logger.Debug("Chunk {ChunkId} → alert path ({ChargeState})", chunk.Id, chunk.ChargeState);
                    return action.ProcessNotification(chunk);
                }, blockOpts);

                var normalBlock = new TransformBlock<AetherChunk, AetherChunk>(chunk =>
                {
                    logger.Debug("Chunk {ChunkId} → normal path ({ChargeState})", chunk.Id, chunk.ChargeState);
                    return action.ProcessNotification(chunk);
                }, blockOpts);

                var staleAlert = new TransformBlock<AetherChunk, AetherChunk?>(chunk =>
                {
                    var result = chunk.FilterStale(opts.AlertStaleMs);
                    if (result is null)
                        logger.Warning("Alert chunk {ChunkId} discarded — stale by more than {MaxAgeMs}ms", chunk.Id,
                            opts.AlertStaleMs);
                    return result;
                }, blockOpts);

                var staleNormal = new TransformBlock<AetherChunk, AetherChunk?>(chunk =>
                {
                    var result = chunk.FilterStale(opts.NormalStaleMs);
                    if (result is null)
                        logger.Debug("Normal chunk {ChunkId} discarded — stale by more than {MaxAgeMs}ms", chunk.Id,
                            opts.NormalStaleMs);
                    return result;
                }, blockOpts);

                var consumeAlert = new ActionBlock<AetherChunk?>(action.Sink, blockOpts);
                var consumeNormal = new ActionBlock<AetherChunk?>(action.Sink, blockOpts);

                // Routing from source
                _pipeline.LinkTo(alertBlock, linkOpts, c => c.IsNotUnknown() && c.IsAlert());
                _pipeline.LinkTo(normalBlock, linkOpts, c => c.IsNotUnknown() && c.IsNormal());
                _pipeline.LinkTo(DataflowBlock.NullTarget<AetherChunk>(), c => c.IsUnknown());

                // Alert path
                alertBlock.LinkTo(staleAlert, linkOpts);
                staleAlert.LinkTo(consumeAlert, linkOpts, c => c != null);
                staleAlert.LinkTo(DataflowBlock.NullTarget<AetherChunk?>());

                // Normal path
                normalBlock.LinkTo(staleNormal, linkOpts);
                staleNormal.LinkTo(consumeNormal, linkOpts, c => c != null);
                staleNormal.LinkTo(DataflowBlock.NullTarget<AetherChunk?>());

                _completion = Task.WhenAll(consumeAlert.Completion, consumeNormal.Completion);

                _isRunning = true;
                logger.Info("AetherPipeline started");
            }
            catch (Exception e)
            {
                logger.Error(e, "Failed to start AetherPipeline");
                _pipeline?.Complete();
                throw;
            }
        }
    }

    public async Task StopAsync()
    {
        lock (this)
        {
            if (!IsRunning()) return;

            logger.Info("AetherPipeline stopping");
            _pipeline?.Complete();
            _pipeline = null;
            _isRunning = false;
        }

        await _completion.ConfigureAwait(false);
        logger.Info("AetherPipeline stopped");
    }

    public Task OfferAsync(params AetherChunk[] chunks)
    {
        if (!IsRunning())
        {
            logger.Warning("OfferAsync called on a pipeline that is not running");
            throw new InvalidOperationException("Pipeline is not running");
        }

        logger.Debug("Offering {Count} chunk(s) to pipeline", chunks.Length);
        return Task.WhenAll(chunks.Select(c => _pipeline!.SendAsync(c)));
    }

    public bool IsRunning() => _isRunning;
}