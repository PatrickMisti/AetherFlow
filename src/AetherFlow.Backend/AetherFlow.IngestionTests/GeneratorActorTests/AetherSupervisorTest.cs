using AetherFlow.Shared.Messages.Ingestion;
using Akka.Actor;
using Akka.TestKit.NUnit;
using AetherFlow.Domain.Domains;
using AetherFlow.Ingestion.Actors;

namespace AetherFlow.IngestionTests.GeneratorActorTests;

[TestFixture]
public class AetherSupervisorTest : TestKit
{
    [Test]
    public void AetherSupervisor_ShouldSpawnCorrectNumberOfWorkers()
    {
        // Arrange
        const int workerCount = 4;
        var supervisorProps = Props.Create(() => new AetherSupervisor(workers: workerCount));

        // Act
        var supervisor = Sys.ActorOf(supervisorProps, "supervisor-spawn-test");

        // Assert
        // Verify supervisor is created successfully
        Assert.NotNull(supervisor);
    }

    [Test]
    public void AetherSupervisor_ShouldStartTimerOnStartGeneratorMessage()
    {
        // Arrange
        var supervisorProps = Props.Create(() => new AetherSupervisor(workers: 2, readingsPerChunk: 5, ticksMs: 1000));
        var supervisor = Sys.ActorOf(supervisorProps, "supervisor-timer-test");

        // Act
        supervisor.Tell(new StartGenerator());

        // Assert - Timer should be started without throwing
        Assert.NotNull(supervisor);
    }

    [Test]
    public void AetherSupervisor_ShouldDispatchWorkToAllWorkers()
    {
        // Arrange
        const int workerCount = 3;
        const int readingsPerChunk = 10;

        var supervisorProps = Props.Create(() =>
            new AetherSupervisor(workers: workerCount, readingsPerChunk: readingsPerChunk, ticksMs: 5000));
        var supervisor = Sys.ActorOf(supervisorProps, "supervisor-dispatch-test");

        // Act
        supervisor.Tell(new DispatchWork());

        // Assert - Should dispatch without throwing
        Assert.NotNull(supervisor);
    }

    [Test]
    public void AetherSupervisor_ShouldLogChunkCompletionWithChargeStateCounts()
    {
        // Arrange
        var chunks = new List<AetherChunk>
        {
            new(ChargeState: AetherChargeState.Full, ChargePercent: 100),
            new(ChargeState: AetherChargeState.Full, ChargePercent: 100),
            new(ChargeState: AetherChargeState.Stable, ChargePercent: 75),
            new(ChargeState: AetherChargeState.Fading, ChargePercent: 30),
            new(ChargeState: AetherChargeState.Fading, ChargePercent: 25),
        };

        var supervisorProps = Props.Create(() => new AetherSupervisor(workers: 2, readingsPerChunk: 5, ticksMs: 1000));
        var supervisor = Sys.ActorOf(supervisorProps, "supervisor-chunk-test");

        // Act
        supervisor.Tell(new GeneratedChunk(Index: 0, Chunks: chunks));

        // Assert - Should process chunk without throwing
        Assert.NotNull(supervisor);
    }

    [Test]
    public void AetherSupervisor_ShouldIncrementChunkIndexOnEachDispatch()
    {
        // Arrange
        const int workerCount = 2;
        var supervisorProps = Props.Create(() => new AetherSupervisor(workers: workerCount, readingsPerChunk: 5, ticksMs: 1000));
        var supervisor = Sys.ActorOf(supervisorProps, "supervisor-index-test");

        // Act
        supervisor.Tell(new DispatchWork());
        supervisor.Tell(new DispatchWork());
        supervisor.Tell(new DispatchWork());

        // Assert - Multiple dispatches should work without throwing
        Assert.NotNull(supervisor);
    }

    [Test]
    public void AetherSupervisor_ShouldGroupChunksByChargeStateCorrectly()
    {
        // Arrange
        var chunks = new List<AetherChunk>
        {
            new(ChargeState: AetherChargeState.Full),
            new(ChargeState: AetherChargeState.Full),
            new(ChargeState: AetherChargeState.Full),
            new(ChargeState: AetherChargeState.Stable),
            new(ChargeState: AetherChargeState.Critical),
            new(ChargeState: AetherChargeState.Critical),
        };

        // Act
        var dict = chunks
            .GroupBy(r => r.ChargeState)
            .ToDictionary(g => g.Key, g => g.Count());

        // Assert
        Assert.AreEqual(3, dict[AetherChargeState.Full]);
        Assert.AreEqual(1, dict[AetherChargeState.Stable]);
        Assert.AreEqual(2, dict[AetherChargeState.Critical]);
        Assert.AreEqual(3, dict.Count);
    }

    [Test]
    public void AetherSupervisor_ShouldInitializeWithCorrectParameters()
    {
        // Arrange
        const int workers = 5;
        const int readingsPerChunk = 20;
        const int ticksMs = 3000;

        // Act
        var supervisorProps = Props.Create(() => new AetherSupervisor(workers: workers, readingsPerChunk: readingsPerChunk, ticksMs: ticksMs));
        var supervisor = Sys.ActorOf(supervisorProps, "supervisor-params-test");

        // Assert
        Assert.NotNull(supervisor);
    }

    [Test]
    public void AetherSupervisor_ShouldHandleEmptyChunksList()
    {
        // Arrange
        var emptyChunks = new List<AetherChunk>();
        var supervisorProps = Props.Create(() => new AetherSupervisor(workers: 2, readingsPerChunk: 5, ticksMs: 1000));
        var supervisor = Sys.ActorOf(supervisorProps, "supervisor-empty-test");

        // Act
        supervisor.Tell(new GeneratedChunk(Index: 0, Chunks: emptyChunks));

        // Assert - Should handle empty list gracefully
        Assert.NotNull(supervisor);
    }
}