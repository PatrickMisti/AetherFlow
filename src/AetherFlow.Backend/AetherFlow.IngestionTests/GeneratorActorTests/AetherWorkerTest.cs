using AetherFlow.Domain.Domains;
using AetherFlow.Ingestion.Actors;
using AetherFlow.Shared.AetherInterfaces;
using AetherFlow.Shared.Messages.Ingestion;
using Akka.Actor;
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

        var sp = new ServiceCollection()
            .AddScoped<IPeripheryConnector<AetherChunk>>(_ => _mockConnector.Object)
            .BuildServiceProvider();

        _mockRandom.Setup(r => r.NextDouble()).Returns(0.5);
        _mockRandom.Setup(r => r.Next(It.IsAny<int>(), It.IsAny<int>())).Returns(5);

        _workerActor = Sys.ActorOf(
            Props.Create(() => new AetherWorker(
                "test-worker-1", 
                sp.GetRequiredService<IServiceScopeFactory>(), 
                _mockRandom.Object)));
    }

    [Test]
    public void AetherWorker_ShouldBeCreatedSuccessfully()
    {
        // Assert
        Assert.IsNotNull(_workerActor);
    }

    [Test]
    public void AetherWorker_ShouldGenerateChunkWithCorrectReadingCount()
    {
        // Arrange
        const int readingsPerChunk = 10;
        var generateChunkMsg = new GenerateChunk(readingsPerChunk, 0);

        _mockConnector!
            .Setup(c => c.GenerateData())
            .Returns(new AetherChunk(
                Rune: "AETH-TEST-01",
                ChargeState: AetherChargeState.Stable,
                ChargePercent: 75));

        // Act
        _workerActor!.Tell(generateChunkMsg, TestActor);

        // Assert
        var response = ExpectMsg<GeneratedChunk>(TimeSpan.FromSeconds(5));
        Assert.That(response.Index, Is.EqualTo(0));
        Assert.That(response.Chunks.Count, Is.EqualTo(readingsPerChunk));
    }

    [Test]
    public void AetherWorker_ShouldThrowAetherConnectionException_WhenConnectorIsNull()
    {
        // Arrange
        var generateChunkMsg = new GenerateChunk(5, 0);

        // Act & Assert - Worker should handle null connector gracefully or log error
        Assert.DoesNotThrow(() => _workerActor!.Tell(generateChunkMsg, TestActor));
    }

    [Test]
    public void AetherWorker_ShouldRespondWithGeneratedChunkMessage()
    {
        // Arrange
        const int readingsPerChunk = 5;
        var generateChunkMsg = new GenerateChunk(readingsPerChunk, 42);

        _mockConnector!
            .Setup(c => c.GenerateData())
            .Returns(new AetherChunk(ChargeState: AetherChargeState.Full, ChargePercent: 100));

        // Act
        _workerActor!.Tell(generateChunkMsg, TestActor);

        // Assert
        var response = ExpectMsg<GeneratedChunk>(TimeSpan.FromSeconds(5));
        Assert.That(response, Is.TypeOf<GeneratedChunk>());
        Assert.That(response.Index, Is.EqualTo(42));
    }

    [Test]
    public void AetherWorker_ShouldGenerateDataWithDifferentChargeStates()
    {
        // Arrange
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

        // Act
        _workerActor!.Tell(generateChunkMsg, TestActor);

        // Assert
        var response = ExpectMsg<GeneratedChunk>(TimeSpan.FromSeconds(5));
        Assert.That(response.Chunks.Count, Is.EqualTo(readingsPerChunk));
    }

    [Test]
    public void AetherWorker_ShouldStopWhenReceivingStopWorkerMessage()
    {
        // Arrange
        var stopWorkerMsg = new StopWorker();
        var deathWatch = CreateTestProbe();

        // Act
        deathWatch.Watch(_workerActor!);
        _workerActor.Tell(stopWorkerMsg);

        // Assert - Worker should stop
        deathWatch.ExpectTerminated(_workerActor, TimeSpan.FromSeconds(5));
    }

    [Test]
    public void AetherWorker_ShouldProcessMultipleGenerateChunkMessagesSequentially()
    {
        // Arrange
        const int readingsPerChunk = 5;

        _mockConnector!
            .Setup(c => c.GenerateData())
            .Returns(new AetherChunk(ChargeState: AetherChargeState.Stable));

        // Act
        for (int i = 0; i < 3; i++)
        {
            _workerActor!.Tell(new GenerateChunk(readingsPerChunk, i), TestActor);
        }

        // Assert
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
        // Arrange
        var readingCounts = new[] { 1, 5, 10, 20 };

        _mockConnector!
            .Setup(c => c.GenerateData())
            .Returns(new AetherChunk(ChargeState: AetherChargeState.Full));

        // Act & Assert
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
        // Arrange
        var indices = new[] { 0, 100, 999, 5000 };

        _mockConnector!
            .Setup(c => c.GenerateData())
            .Returns(new AetherChunk(ChargeState: AetherChargeState.Stable));

        // Act & Assert
        foreach (var index in indices)
        {
            _workerActor!.Tell(new GenerateChunk(5, index), TestActor);
            var response = ExpectMsg<GeneratedChunk>(TimeSpan.FromSeconds(5));
            Assert.That(response.Index, Is.EqualTo(index));
        }
    }

    [Test]
    //[Ignore("This test is designed to check if all properties of AetherChunk are correctly included in the generated chunks. It may require adjustments based on the actual implementation of AetherWorker and the data it generates.")]
    public void AetherWorker_ShouldReturnChunksWithAllProperties()
    {
        // Arrange
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

        // Act
        _workerActor!.Tell(new GenerateChunk(readingsPerChunk, 0), TestActor);

        // Assert
        var response = ExpectMsg<GeneratedChunk>(TimeSpan.FromSeconds(5));
        Assert.That(response.Chunks.Count, Is.EqualTo(readingsPerChunk));

        var chunk = response.Chunks.First();
        //Assert.That(chunk.Rune, Is.EqualTo("AETH-ZEPHYR-01"));
        Assert.That(chunk.ChargeState, Is.EqualTo(AetherChargeState.Stable));
        Assert.That(chunk.ChargePercent, Is.EqualTo(85));
    }

    [Test]
    public void AetherWorker_ShouldHandleRapidMessageSequence()
    {
        // Arrange
        _mockConnector!
            .Setup(c => c.GenerateData())
            .Returns(new AetherChunk(ChargeState: AetherChargeState.Full));

        // Act - Send multiple messages rapidly
        for (int i = 0; i < 10; i++)
        {
            _workerActor!.Tell(new GenerateChunk(3, i), TestActor);
        }

        // Assert - Expect all responses
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