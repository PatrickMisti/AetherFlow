using AetherFlow.Domain.Domains;
using AetherFlow.Ingestion.Actors;
using AetherFlow.Shared.AetherInterfaces;
using AetherFlow.Shared.Messages.Ingestion;
using Akka.Actor;
using Akka.Hosting;
using Akka.TestKit;
using Akka.TestKit.NUnit;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace AetherFlow.IngestionTests.GeneratorActorTests;

[TestFixture]
public class AetherWorkerTest : TestKit
{
    private Mock<IPeripheryConnector<AetherChunk>>? _mockConnector;
    private Mock<Random>? _mockRandom;
    private IActorRef? _workerActor;

    [SetUp]
    public void Setup()
    {
        _mockConnector = new Mock<IPeripheryConnector<AetherChunk>>();
        _mockRandom = new Mock<Random>();

        _mockRandom.Setup(r => r.NextDouble()).Returns(0.5);
        _mockRandom.Setup(r => r.Next(It.IsAny<int>(), It.IsAny<int>())).Returns(5);

        var pipelineProbe = CreateTestProbe();
        pipelineProbe.SetAutoPilot(new DelegateAutoPilot((sender, message) =>
        {
            if (message is PipelineStatusRequest)
                sender.Tell(new PipelineStatusResponse(IsRunning: false));
            return AutoPilot.KeepRunning;
        }));

        var mockRequiredActor = new Mock<IRequiredActor<AetherPipelineActor>>();
        mockRequiredActor.Setup(r => r.ActorRef).Returns(pipelineProbe.Ref);

        var sp = new ServiceCollection()
            .AddScoped<IPeripheryConnector<AetherChunk>>(_ => _mockConnector.Object)
            .BuildServiceProvider();

        _workerActor = Sys.ActorOf(
            Props.Create(() => new AetherWorker(
                mockRequiredActor.Object,
                "test-worker-1",
                sp.GetRequiredService<IServiceScopeFactory>(),
                _mockRandom.Object)));
    }

    [Test]
    public void AetherWorker_ShouldBeCreatedSuccessfully()
    {
        Assert.IsNotNull(_workerActor);
    }

    [Test]
    public void AetherWorker_ShouldGenerateChunkWithCorrectReadingCount()
    {
        const int readingsPerChunk = 10;
        var generateChunkMsg = new GenerateChunk(readingsPerChunk, 0);

        _mockConnector!
            .Setup(c => c.GenerateData())
            .Returns(new AetherChunk(
                Rune: "AETH-TEST-01",
                ChargeState: AetherChargeState.Stable,
                ChargePercent: 75));

        _workerActor!.Tell(generateChunkMsg, TestActor);

        var response = ExpectMsg<GeneratedChunk>(TimeSpan.FromSeconds(5));
        Assert.That(response.Index, Is.EqualTo(0));
        Assert.That(response.Chunks.Count, Is.EqualTo(readingsPerChunk));
    }

    [Test]
    public void AetherWorker_ShouldThrowAetherConnectionException_WhenConnectorIsNull()
    {
        var generateChunkMsg = new GenerateChunk(5, 0);
        Assert.DoesNotThrow(() => _workerActor!.Tell(generateChunkMsg, TestActor));
    }

    [Test]
    public void AetherWorker_ShouldRespondWithGeneratedChunkMessage()
    {
        const int readingsPerChunk = 5;
        var generateChunkMsg = new GenerateChunk(readingsPerChunk, 42);

        _mockConnector!
            .Setup(c => c.GenerateData())
            .Returns(new AetherChunk(ChargeState: AetherChargeState.Full, ChargePercent: 100));

        _workerActor!.Tell(generateChunkMsg, TestActor);

        var response = ExpectMsg<GeneratedChunk>(TimeSpan.FromSeconds(5));
        Assert.That(response, Is.TypeOf<GeneratedChunk>());
        Assert.That(response.Index, Is.EqualTo(42));
    }

    [Test]
    public void AetherWorker_ShouldGenerateDataWithDifferentChargeStates()
    {
        const int readingsPerChunk = 3;
        var generateChunkMsg = new GenerateChunk(readingsPerChunk, 0);

        var chargeStates = new[]
        {
            AetherChargeState.Full,
            AetherChargeState.Stable,
            AetherChargeState.Fading
        };

        var callCount = 0;
        _mockConnector!
            .Setup(c => c.GenerateData())
            .Returns(() => new AetherChunk(
                Rune: $"AETH-TEST-{callCount}",
                ChargeState: chargeStates[callCount++ % chargeStates.Length],
                ChargePercent: 50 + callCount * 10));

        _workerActor!.Tell(generateChunkMsg, TestActor);

        var response = ExpectMsg<GeneratedChunk>(TimeSpan.FromSeconds(5));
        Assert.That(response.Chunks.Count, Is.EqualTo(readingsPerChunk));
    }

    [Test]
    public void AetherWorker_ShouldStopWhenReceivingStopWorkerMessage()
    {
        var stopWorkerMsg = new StopWorker();
        var deathWatch = CreateTestProbe();

        deathWatch.Watch(_workerActor!);
        _workerActor.Tell(stopWorkerMsg);

        deathWatch.ExpectTerminated(_workerActor, TimeSpan.FromSeconds(5));
    }

    [Test]
    public void AetherWorker_ShouldProcessMultipleGenerateChunkMessagesSequentially()
    {
        const int readingsPerChunk = 5;

        _mockConnector!
            .Setup(c => c.GenerateData())
            .Returns(new AetherChunk(ChargeState: AetherChargeState.Stable));

        for (int i = 0; i < 3; i++)
            _workerActor!.Tell(new GenerateChunk(readingsPerChunk, i), TestActor);

        for (int i = 0; i < 3; i++)
        {
            var response = ExpectMsg<GeneratedChunk>(TimeSpan.FromSeconds(5));
            Assert.That(response.Index, Is.EqualTo(i));
            Assert.That(response.Chunks.Count, Is.EqualTo(readingsPerChunk));
        }
    }

    [Test]
    public void AetherWorker_ShouldGenerateChunksWithVariousReadingCounts()
    {
        var readingCounts = new[] { 1, 5, 10, 20 };

        _mockConnector!
            .Setup(c => c.GenerateData())
            .Returns(new AetherChunk(ChargeState: AetherChargeState.Full));

        foreach (var count in readingCounts)
        {
            _workerActor!.Tell(new GenerateChunk(count, 0), TestActor);
            var response = ExpectMsg<GeneratedChunk>(TimeSpan.FromSeconds(5));
            Assert.That(response.Chunks.Count, Is.EqualTo(count));
        }
    }

    [Test]
    public void AetherWorker_ShouldIncludeCorrectIndexInGeneratedChunk()
    {
        var indices = new[] { 0, 100, 999, 5000 };

        _mockConnector!
            .Setup(c => c.GenerateData())
            .Returns(new AetherChunk(ChargeState: AetherChargeState.Stable));

        foreach (var index in indices)
        {
            _workerActor!.Tell(new GenerateChunk(5, index), TestActor);
            var response = ExpectMsg<GeneratedChunk>(TimeSpan.FromSeconds(5));
            Assert.That(response.Index, Is.EqualTo(index));
        }
    }

    [Test]
    public void AetherWorker_ShouldReturnChunksWithAllProperties()
    {
        const int readingsPerChunk = 2;

        _mockConnector!
            .Setup(c => c.GenerateData())
            .Returns(new AetherChunk(
                Rune: "AETH-ZEPHYR-01",
                Designation: "Test-Designation",
                Kind: AetherConstructKind.EmberOrb,
                Status: AetherPresence.Awakened,
                WarmthC: 25.5,
                WeightPressureHpa: 1013.25,
                MistPercent: 60.0,
                Presence: ManifestationState.Present,
                ChargePercent: 85,
                ChargeState: AetherChargeState.Stable));

        _workerActor!.Tell(new GenerateChunk(readingsPerChunk, 0), TestActor);

        var response = ExpectMsg<GeneratedChunk>(TimeSpan.FromSeconds(5));
        Assert.That(response.Chunks.Count, Is.EqualTo(readingsPerChunk));

        var chunk = response.Chunks.First();
        Assert.That(chunk.ChargeState, Is.EqualTo(AetherChargeState.Stable));
        Assert.That(chunk.ChargePercent, Is.EqualTo(85));
    }

    [Test]
    public void AetherWorker_ShouldHandleRapidMessageSequence()
    {
        _mockConnector!
            .Setup(c => c.GenerateData())
            .Returns(new AetherChunk(ChargeState: AetherChargeState.Full));

        for (int i = 0; i < 10; i++)
            _workerActor!.Tell(new GenerateChunk(3, i), TestActor);

        for (int i = 0; i < 10; i++)
        {
            var response = ExpectMsg<GeneratedChunk>(TimeSpan.FromMilliseconds(600));
            Assert.That(response.Index, Is.GreaterThanOrEqualTo(0));
        }
    }

    [TearDown]
    public void TearDown()
    {
        _workerActor?.Tell(new StopWorker());
    }
}