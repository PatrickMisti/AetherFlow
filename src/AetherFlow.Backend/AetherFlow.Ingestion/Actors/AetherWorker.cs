using AetherFlow.Domain.Domains;
using AetherFlow.Ingestion.Exceptions;
using AetherFlow.Shared.AetherInterfaces;
using AetherFlow.Shared.Messages.Ingestion;
using Akka.Actor;
using Akka.Event;

namespace AetherFlow.Ingestion.Actors;

public class AetherWorker : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();

    private readonly IPeripheryConnector<AetherChunk>? _connector;
    private readonly string _workerId;
    private readonly Random _random;

    public AetherWorker(string workerId, IServiceScopeFactory scope, Random? random = null)
    {
        _workerId = workerId;
        _random = random ?? Random.Shared;

        //var provider = PeripheryProvider.GetProvider();
        _connector = scope.CreateScope().ServiceProvider.GetService<IPeripheryConnector<AetherChunk>>();

        Receive<GenerateChunk>(HandleGenerateChunk);
        Receive<StopWorker>(_ =>
        {
            _log.Info("Worker {WorkerId} received StopWorker message.", _workerId);
            Context.Stop(Self);
        });
    }

    private void HandleGenerateChunk(GenerateChunk chunk)
    {
        _log.Debug("Worker {WorkerId} received GenerateChunk message with ReadingsPerChunk: {ReadingsPerChunk} and Index: {Index}", _workerId, chunk.ReadingsPerChunk, chunk.Index);

        if (_connector == null)
        {
            _log.Error("Worker {WorkerId} has no connector available to generate chunk.", _workerId);
            throw new AetherConnectionException("Connection to periphery is not available.");
        }
#if DEBUG
        if (_random.NextDouble() < 0.1)
        {
            _log.Error("Worker {WorkerId} simulated a failure.", _workerId);
            throw new AetherSimulationException("Simulated failure.");
        }

        Task.Delay(_random.Next(100, 500)).Wait(); // Simulate variable processing time
#endif
        var genChunk = Enumerable.Range(0, chunk.ReadingsPerChunk).AsParallel().Select(_ => _connector.GenerateData());


        Sender.Tell(new GeneratedChunk(chunk.Index, genChunk.ToList()));
    }

    protected override void PreStart()
    {
        _connector?.Connect();
        _log.Debug("[{WorkerId}] Worker online.", _workerId);
    }

    protected override void PostStop()
    {
        _connector?.Disconnect();
        _log.Debug("[{WorkerId}] Worker offline.", _workerId);
    }

    protected override void PreRestart(Exception reason, object message)
    {
        _log.Warning("[{WorkerId}] Restarting after fault: {Reason}", _workerId, reason.Message);
        base.PreRestart(reason, message);
    }

    protected override void PostRestart(Exception reason)
    {
        _log.Info("[{WorkerId}] Resumed after restart.", _workerId);
        base.PostRestart(reason);
    }
}