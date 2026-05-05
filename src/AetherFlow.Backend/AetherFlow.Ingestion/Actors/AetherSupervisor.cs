using AetherFlow.Domain.Domains;
using AetherFlow.Ingestion.Exceptions;
using AetherFlow.Shared.Messages.Ingestion;
using Akka.Actor;
using Akka.DependencyInjection;
using Akka.Event;

namespace AetherFlow.Ingestion.Actors;

public class AetherSupervisor : ReceiveActor, IWithTimers
{
    private readonly ILoggingAdapter _log = Context.GetLogger();

    public ITimerScheduler? Timers { get; set; }
    private IReadOnlyList<IActorRef> _workers = [];

    private int _chunkIndex;
    private readonly int _workerCount;
    private readonly int _readingPerChunk;
    private readonly TimeSpan _tickMs;

    public AetherSupervisor(int workers = 4, int readingsPerChunk = 10, int ticksMs = 2000)
    {
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
        _log.Info("Received completed chunk {ChunkIndex} with {ReadingCount} readings",
            msg.Index, chunk.Count());

        var dict = chunk
            .GroupBy(r => r.ChargeState)
            .ToDictionary(g => g.Key, g => g.Count());

        foreach (var kvp in dict)
            _log.Info("\t{ChargeState}: {Count} readings", kvp.Key, kvp.Value);
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