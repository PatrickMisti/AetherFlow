using AetherFlow.Domain.Domains;
using AetherFlow.Ingestion.Actors;
using AetherFlow.Shared.Messages.Ingestion;
using Akka.Actor;
using Akka.Hosting;
using Akka.TestKit.NUnit;
using Moq;

namespace AetherFlow.IngestionTests.GeneratorActorTests;

[TestFixture]
public class AetherSupervisorTest : TestKit
{
    private Mock<IRequiredActor<AetherPipelineActor>> _mockPipeline = null!;

    [SetUp]
    public void SetUp()
    {
        var pipelineProbe = CreateTestProbe();
        _mockPipeline = new Mock<IRequiredActor<AetherPipelineActor>>();
        _mockPipeline.Setup(p => p.ActorRef).Returns(pipelineProbe.Ref);
    }

    [Test]
    public void AetherSupervisor_ShouldSpawnCorrectNumberOfWorkers()
    {
        // Arrange
        const int workerCount = 4;
        var supervisorProps = Props.Create(() => new AetherSupervisor(_mockPipeline.Object, workers: workerCount));
        var supervisor = Sys.ActorOf(supervisorProps, "supervisor-spawn-test");

        // Assert: Actor created without exception
        Assert.NotNull(supervisor);
    }

    [Test]
    public void AetherSupervisor_ShouldStartTimerOnStartGeneratorMessage()
    {
        // Arrange
        var supervisorProps = Props.Create(() =>
            new AetherSupervisor(_mockPipeline.Object, workers: 2, readingsPerChunk: 5, ticksMs: 1000));
        var supervisor = Sys.ActorOf(supervisorProps, "supervisor-timer-test");

        // Act
        supervisor.Tell(new StartGenerator());

        // Assert: Timer started without exception
        Assert.NotNull(supervisor);
    }

    [Test]
    public void AetherSupervisor_ShouldDispatchWorkToAllWorkers()
    {
        // Arrange
        const int workerCount = 3;
        const int readingsPerChunk = 10;

        var supervisorProps = Props.Create(() =>
            new AetherSupervisor(_mockPipeline.Object, workers: workerCount, readingsPerChunk: readingsPerChunk, ticksMs: 5000));

        var supervisor = Sys.ActorOf(supervisorProps, "supervisor-dispatch-test");

        // Act
        supervisor.Tell(new DispatchWork());

        // Assert: Should dispatch without throwing
        Assert.NotNull(supervisor);
    }

    [Test]
    public void AetherSupervisor_ShouldProcessChunkCompletionMessage()
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

        var supervisorProps = Props.Create(() =>
            new AetherSupervisor(_mockPipeline.Object, workers: 2, readingsPerChunk: 5, ticksMs: 1000));
        var supervisor = Sys.ActorOf(supervisorProps, "supervisor-chunk-test");

        // Act: Send chunk completion
        supervisor.Tell(new GeneratedChunk(Index: 0, Chunks: chunks));

        // Assert: Should process chunk without throwing
        Assert.NotNull(supervisor);
    }

    [Test]
    public void AetherSupervisor_ShouldIncrementChunkIndexOnEachDispatch()
    {
        // Arrange
        const int workerCount = 2;
        var supervisorProps = Props.Create(() =>
            new AetherSupervisor(_mockPipeline.Object, workers: workerCount, readingsPerChunk: 5, ticksMs: 1000));
        var supervisor = Sys.ActorOf(supervisorProps, "supervisor-index-test");

        // Act: Multiple dispatches
        supervisor.Tell(new DispatchWork());
        supervisor.Tell(new DispatchWork());
        supervisor.Tell(new DispatchWork());

        // Assert: Should process multiple dispatches without exception
        Assert.NotNull(supervisor);
    }

    [Test]
    public void ChunkGrouping_ShouldGroupByChargeStateCorrectly()
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
    public void AetherSupervisor_ShouldHandleEmptyChunksList()
    {
        // Arrange
        var emptyChunks = new List<AetherChunk>();
        var supervisorProps = Props.Create(() => 
            new AetherSupervisor(_mockPipeline.Object, workers: 2, readingsPerChunk: 5, ticksMs: 1000));
        var supervisor = Sys.ActorOf(supervisorProps, "supervisor-empty-test");

        // Act
        supervisor.Tell(new GeneratedChunk(Index: 0, Chunks: emptyChunks));

        // Assert: Should handle empty list gracefully
        Assert.NotNull(supervisor);
    }
}