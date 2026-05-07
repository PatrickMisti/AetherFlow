using AetherFlow.Ingestion.Exceptions;
using AetherFlow.Shared.Messages.Ingestion;
using Akka.Actor;
using Akka.DependencyInjection;
using Akka.Event;
using Akka.Hosting;

namespace AetherFlow.Ingestion.Actors;

public class AetherSupervisor : ReceiveActor, IWithTimers
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly IActorRef _pipelineRef;

    public ITimerScheduler? Timers { get; set; }
    private IReadOnlyList<IActorRef> _workers = [];

    private int _chunkIndex;
    private readonly int _workerCount;
    private readonly int _readingPerChunk;
    private readonly TimeSpan _tickMs;

    public AetherSupervisor(IRequiredActor<AetherPipelineActor> pipeline, int workers = 4, int readingsPerChunk = 10,
        int ticksMs = 2000)
    {
        _pipelineRef = pipeline.ActorRef;
        _workerCount = workers;
        _readingPerChunk = readingsPerChunk;
        _tickMs = TimeSpan.FromMilliseconds(ticksMs);

        Receive<StartGenerator>(_ => OnStartTimer());
        Receive<DispatchWork>(_ => HandleDispatchWork());
        Receive<GeneratedChunk>(OnChunkCompleted);
    }

    private void OnStartTimer()
    {
        Timers?.StartPeriodicTimer(
            key: "AetherSupervisorTick",
            msg: new DispatchWork(),
            interval: _tickMs
        );
    }

    private void HandleDispatchWork()
    {
        _log.Debug("Dispatching work to workers...");

        foreach (var worker in _workers)
        {
            worker.Tell(new GenerateChunk(_readingPerChunk, _chunkIndex++));
            _log.Debug("Dispatched work to {WorkerPath}", worker.Path);
        }
    }

    private void OnChunkCompleted(GeneratedChunk msg)
    {
        var chunk = msg.Chunks;
        _log.Debug("Received completed chunk {ChunkIndex} with {ReadingCount} readings",
            msg.Index, chunk.Count());

        var dict = chunk
            .GroupBy(r => r.ChargeState)
            .ToDictionary(g => g.Key, g => g.Count());

        foreach (var kvp in dict)
            _log.Debug("\t{ChargeState}: {Count} readings", kvp.Key, kvp.Value);
    }

    protected override void PreStart()
    {
        _log.Info("══════════════════════════════════════════════");
        _log.Info("  AETHER NETWORK SUPERVISOR  —  INITIALISING");
        _log.Info("══════════════════════════════════════════════");
        _log.Info("Spawning {WorkerCount} worker nodes...", _workerCount);

        _workers = Enumerable
            .Range(0, _workerCount).Select(i =>
            {
                var worker =
                    Context.ActorOf(DependencyResolver.For(Context.System).Props<AetherWorker>($"aether-worker-{i}"));
                _log.Debug("Spawned worker: {WorkerPath}", worker.Path);
                return worker;
            })
            .ToList();

        _log.Info("Send message to start pipeline");
        _pipelineRef.Tell(StartPipelineMessage.Instance);
    }

    protected override void PostStop()
    {
        _log.Info("══════════════════════════════════════════════");
        _log.Info("  AETHER NETWORK SUPERVISOR  —  SHUTTING DOWN");
        _log.Info("══════════════════════════════════════════════");

        _log.Info("Spawning {WorkerCount} worker nodes...", _workerCount);
        foreach (var worker in _workers)
        {
            _log.Debug("Stopping worker: {WorkerPath}", worker.Path);
            Context.Stop(worker);
        }

        _log.Info("Send message to stop pipeline");
        _pipelineRef.Tell(new StopPipelineMessage());
        base.PostStop();
    }

    protected override SupervisorStrategy SupervisorStrategy()
        => new OneForOneStrategy(
            maxNrOfRetries: 10,
            withinTimeRange: TimeSpan.FromMilliseconds(200),
            localOnlyDecider: ex => ex switch
            {
                AetherSimulationException => Directive.Restart,
                _ => Directive.Escalate
            });
}