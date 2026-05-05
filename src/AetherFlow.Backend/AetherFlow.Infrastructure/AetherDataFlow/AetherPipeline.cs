using AetherFlow.Domain.Domains;
using AetherFlow.Shared.AetherInterfaces;
using AetherFlow.Shared.Pipeline;
using Serilog;
using System.Threading.Tasks.Dataflow;
using AetherFlow.Infrastructure.Utils;

namespace AetherFlow.Infrastructure.AetherDataFlow;

public class AetherPipeline(ILogger logger) : IAetherPipeline
{
    private BufferBlock<AetherChunk>? _pipeline;
    private bool _isRunning;

    // private Task _completion = Task.CompletedTask;
    private readonly ILogger _log = logger;

    public void Start(IAetherChunkPipelineAction action, AetherPipelineOptions? options)
    {
        lock (this)
        {
            if (IsRunning()) return;

            _pipeline = null;

            try
            {
                var opts = options ?? new AetherPipelineOptions();
                var linkOpts = new DataflowLinkOptions { PropagateCompletion = true };
                var blockOpts = new ExecutionDataflowBlockOptions { BoundedCapacity = opts.BoundedCapacity };

                _log.Information("AetherPipeline starting — Capacity={Capacity}, AlertStaleMs={AlertMs}, NormalStaleMs={NormalMs}",
                    opts.BoundedCapacity, opts.AlertStaleMs, opts.NormalStaleMs);

                // Blocks
                _pipeline = new BufferBlock<AetherChunk>(blockOpts);

                var alertBlock = new TransformBlock<AetherChunk, AetherChunk>(
                    transform: chunk =>
                    {
                        _log.Debug("Chunk {ChunkId} → alert path ({ChargeState})", chunk.Id, chunk.ChargeState);
                        return action.ProcessNotification(chunk);
                    },
                    dataflowBlockOptions: blockOpts);

                var normalBlock = new TransformBlock<AetherChunk, AetherChunk>(
                    transform: chunk =>
                    {
                        _log.Debug("Chunk {ChunkId} → normal path ({ChargeState})", chunk.Id, chunk.ChargeState);
                        return action.ProcessNotification(chunk);
                    },
                    dataflowBlockOptions: blockOpts);

                var staleAlert = new TransformBlock<AetherChunk, AetherChunk?>(
                    transform: chunk =>
                    {
                        var result = chunk.FilterStale(opts.AlertStaleMs);
                        if (result is null)
                            _log.Warning("Alert chunk {ChunkId} discarded — stale by more than {MaxAgeMs}ms", chunk.Id, opts.AlertStaleMs);
                        return result;
                    },
                    dataflowBlockOptions: blockOpts);

                var staleNormal = new TransformBlock<AetherChunk, AetherChunk?>(
                    transform: chunk =>
                    {
                        var result = chunk.FilterStale(opts.NormalStaleMs);
                        if (result is null)
                            _log.Debug("Normal chunk {ChunkId} discarded — stale by more than {MaxAgeMs}ms", chunk.Id, opts.NormalStaleMs);
                        return result;
                    },
                    dataflowBlockOptions: blockOpts);

                var consumeAlert = new ActionBlock<AetherChunk?>(
                    action: action.Sink,
                    dataflowBlockOptions: blockOpts);

                var consumeNormal = new ActionBlock<AetherChunk?>(
                    action: action.Sink,
                    dataflowBlockOptions: blockOpts);

                var dropUnknown = new ActionBlock<AetherChunk>(chunk =>
                    _log.Debug("Chunk {ChunkId} discarded — unknown state (Presence={Presence}, ChargeState={ChargeState})",
                        chunk.Id, chunk.Presence, chunk.ChargeState));

                // Links
                _pipeline.LinkTo(alertBlock, linkOpts, c => c.IsNotUnknown() && c.IsAlert());
                _pipeline.LinkTo(normalBlock, linkOpts, c => c.IsNotUnknown() && c.IsNormal());
                // _pipeline.LinkTo(dropUnknown); // catch-all: unknown/unmatched chunks

                alertBlock.LinkTo(staleAlert, linkOpts);
                normalBlock.LinkTo(staleNormal, linkOpts);

                staleAlert.LinkTo(consumeAlert, linkOpts, c => c != null);
                staleAlert.LinkTo(DataflowBlock.NullTarget<AetherChunk?>());

                staleNormal.LinkTo(consumeNormal, linkOpts, c => c != null);
                // Without sink, backpressure would shut down the pipeline
                staleNormal.LinkTo(DataflowBlock.NullTarget<AetherChunk?>());

                // check if needed, but should be fine since all blocks are linked with PropagateCompletion
                Task.WhenAll(consumeAlert.Completion, consumeNormal.Completion);
                _isRunning = true;

                _log.Information("AetherPipeline started");
            }
            catch (Exception e)
            {
                _log.Error(e, "Failed to start AetherPipeline");
                _pipeline?.Complete();
                throw;
            }
        }
    }

    
    private void buildPipeline()
    {
        
    }
    public void Stop()
    {
        if (!IsRunning()) return;

        _log.Information("AetherPipeline stopping");
        _pipeline?.Complete();
        _pipeline = null;
        _isRunning = false;
        _log.Information("AetherPipeline stopped");
    }

    public Task OfferAsync(params AetherChunk[] chunks)
    {
        if (!IsRunning())
        {
            _log.Warning("OfferAsync called on a pipeline that is not running");
            throw new InvalidOperationException("Pipeline is not running");
        }
        _log.Debug("Offering {Count} chunk(s) to pipeline", chunks.Length);
        return Task.WhenAll(chunks.Select(c => _pipeline!.SendAsync(c)));
    }

    public bool IsRunning() => _isRunning;
}