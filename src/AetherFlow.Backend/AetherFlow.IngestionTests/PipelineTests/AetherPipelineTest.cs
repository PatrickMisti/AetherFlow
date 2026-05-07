using AetherFlow.Domain.Domains;
using AetherFlow.Infrastructure.AetherDataFlow;
using AetherFlow.Shared.Pipeline;
using Akka.Event;
using Moq;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

namespace AetherFlow.IngestionTests.PipelineTests;

[TestFixture]
public class AetherPipelineTest
{
    private Mock<IAetherChunkPipelineAction> _mockAction = null!;
    private AetherPipeline _pipeline = null!;

    private static readonly AetherPipelineOptions GenerousOpts =
        new() { AlertStaleMs = 5000, NormalStaleMs = 5000 };

    [SetUp]
    public void SetUp()
    {
        var mockLogger = new Mock<ILoggingAdapter>();

        _mockAction = new Mock<IAetherChunkPipelineAction>();
        _mockAction.Setup(a => a.ProcessNotification(It.IsAny<AetherChunk>()))
            .Returns<AetherChunk>(c => c);

        _pipeline = new AetherPipeline(mockLogger.Object);
    }

    [TearDown]
    public async Task TearDown()
    {
        await _pipeline.StopAsync();
    }

    // Returns a task that completes after Sink has been called `count` times.
    private Task WhenSinked(int count = 1)
    {
        var tcs = new TaskCompletionSource();
        var remaining = count;
        _mockAction.Setup(a => a.Sink(It.IsAny<AetherChunk?>()))
            .Callback<AetherChunk?>(_ =>
            {
                if (Interlocked.Decrement(ref remaining) == 0)
                    tcs.TrySetResult();
            });
        return tcs.Task;
    }

    // Returns a task that completes after ProcessNotification has been called `count` times.
    private Task WhenProcessed(int count = 1)
    {
        var tcs = new TaskCompletionSource();
        var remaining = count;
        _mockAction.Setup(a => a.ProcessNotification(It.IsAny<AetherChunk>()))
            .Returns<AetherChunk>(c => c)
            .Callback<AetherChunk>(_ =>
            {
                if (Interlocked.Decrement(ref remaining) == 0)
                    tcs.TrySetResult();
            });
        return tcs.Task;
    }

    private BufferBlock<AetherChunk> GetBufferBlock()
    {
        var field = typeof(AetherPipeline).GetField("_pipeline", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (BufferBlock<AetherChunk>)field.GetValue(_pipeline)!;
    }

    // --- Control flow ---

    [Test]
    public void Start_SetsIsRunningToTrue()
    {
        _pipeline.Start(_mockAction.Object, null);
        Assert.That(_pipeline.IsRunning(), Is.True);
    }

    [Test]
    public void Start_WhenAlreadyRunning_IsIdempotent()
    {
        _pipeline.Start(_mockAction.Object, null);
        _pipeline.Start(_mockAction.Object, null);
        Assert.That(_pipeline.IsRunning(), Is.True);
    }

    [Test]
    public async Task Stop_SetsIsRunningToFalse()
    {
        _pipeline.Start(_mockAction.Object, null);
        await _pipeline.StopAsync();
        Assert.That(_pipeline.IsRunning(), Is.False);
    }

    [Test]
    public void Stop_WhenNotRunning_DoesNotThrow()
    {
        Assert.DoesNotThrowAsync(async () => await _pipeline.StopAsync());
    }

    [Test]
    public void IsRunning_ReturnsFalseBeforeStart()
    {
        Assert.That(_pipeline.IsRunning(), Is.False);
    }

    // --- OfferAsync ---

    [Test]
    public void OfferAsync_WhenNotRunning_ThrowsInvalidOperationException()
    {
        Assert.Throws<InvalidOperationException>(() => _pipeline.OfferAsync(new AetherChunk()));
    }

    [Test]
    public async Task OfferAsync_WhenRunning_AcceptsChunk()
    {
        var sinked = WhenSinked();
        _pipeline.Start(_mockAction.Object, GenerousOpts);

        await _pipeline.OfferAsync(new AetherChunk(ChargeState: AetherChargeState.Critical, Presence: ManifestationState.Present));

        await sinked.WaitAsync(TimeSpan.FromSeconds(3));
        _mockAction.Verify(a => a.Sink(It.IsNotNull<AetherChunk?>()), Times.Once);
    }

    [Test]
    public async Task OfferAsync_MultipleChunks_AllAccepted()
    {
        var sinked = WhenSinked(count: 3);
        _pipeline.Start(_mockAction.Object, GenerousOpts);

        var chunks = Enumerable.Range(0, 3)
            .Select(_ => new AetherChunk(ChargeState: AetherChargeState.Critical, Presence: ManifestationState.Present))
            .ToArray();

        await _pipeline.OfferAsync(chunks);

        await sinked.WaitAsync(TimeSpan.FromSeconds(5));
        _mockAction.Verify(a => a.Sink(It.IsNotNull<AetherChunk?>()), Times.Exactly(3));
    }

    // --- Routing ---

    [Test]
    public async Task AlertChunk_Critical_IsProcessedAndSinked()
    {
        var sinked = WhenSinked();
        _pipeline.Start(_mockAction.Object, GenerousOpts);

        var chunk = new AetherChunk(ChargeState: AetherChargeState.Critical, Presence: ManifestationState.Present);
        await _pipeline.OfferAsync(chunk);

        await sinked.WaitAsync(TimeSpan.FromSeconds(3));

        _mockAction.Verify(a => a.ProcessNotification(chunk), Times.Once);
        _mockAction.Verify(a => a.Sink(It.IsNotNull<AetherChunk?>()), Times.Once);

        await _pipeline.StopAsync();
    }

    [Test]
    public async Task AlertChunk_Fading_IsProcessedAndSinked()
    {
        var sinked = WhenSinked();
        _pipeline.Start(_mockAction.Object, GenerousOpts);

        var chunk = new AetherChunk(ChargeState: AetherChargeState.Fading, Presence: ManifestationState.Present);
        await _pipeline.OfferAsync(chunk);

        await sinked.WaitAsync(TimeSpan.FromSeconds(3));

        _mockAction.Verify(a => a.ProcessNotification(chunk), Times.Once);
        _mockAction.Verify(a => a.Sink(It.IsNotNull<AetherChunk?>()), Times.Once);
    }

    [Test]
    public async Task NormalChunk_Full_IsProcessedAndSinked()
    {
        var sinked = WhenSinked();
        _pipeline.Start(_mockAction.Object, GenerousOpts);

        var chunk = new AetherChunk(ChargeState: AetherChargeState.Full, Presence: ManifestationState.Present);
        await _pipeline.OfferAsync(chunk);

        await sinked.WaitAsync(TimeSpan.FromSeconds(3));

        _mockAction.Verify(a => a.ProcessNotification(chunk), Times.Once);
        _mockAction.Verify(a => a.Sink(It.IsNotNull<AetherChunk?>()), Times.Once);
    }

    [Test]
    public async Task NormalChunk_Recharging_IsProcessedAndSinked()
    {
        var sinked = WhenSinked();
        _pipeline.Start(_mockAction.Object, GenerousOpts);

        var chunk = new AetherChunk(ChargeState: AetherChargeState.Recharging, Presence: ManifestationState.Present);
        await _pipeline.OfferAsync(chunk);

        await sinked.WaitAsync(TimeSpan.FromSeconds(3));

        _mockAction.Verify(a => a.ProcessNotification(chunk), Times.Once);
        _mockAction.Verify(a => a.Sink(It.IsNotNull<AetherChunk?>()), Times.Once);
    }

    [Test]
    [Ignore("Only if pipe should drop elements")]
    public async Task UnknownChunk_IsDropped_NeitherProcessNorSinkCalled()
    {
        _pipeline.Start(_mockAction.Object, null);

        // Default AetherChunk: Presence=Unknown AND ChargeState=Unknown → IsUnknown() == true
        await _pipeline.OfferAsync(new AetherChunk());

        var buffer = GetBufferBlock();
        buffer.Complete();
        await buffer.Completion;
        await Task.Delay(100);

        _mockAction.Verify(a => a.ProcessNotification(It.IsAny<AetherChunk>()), Times.Never);
        _mockAction.Verify(a => a.Sink(It.IsAny<AetherChunk?>()), Times.Never);
    }

    [Test]
    public async Task StaleAlertChunk_ProcessNotificationCalled_SinkNotCalled()
    {
        var processed = WhenProcessed();
        _pipeline.Start(_mockAction.Object, new AetherPipelineOptions { AlertStaleMs = 100 });

        var chunk = new AetherChunk(ChargeState: AetherChargeState.Critical, Presence: ManifestationState.Present)
            { LastWhisperUtc = DateTime.UtcNow.AddSeconds(-10) };

        await _pipeline.OfferAsync(chunk);

        await processed.WaitAsync(TimeSpan.FromSeconds(3));
        await Task.Delay(100);

        _mockAction.Verify(a => a.ProcessNotification(chunk), Times.Once);
        _mockAction.Verify(a => a.Sink(It.IsAny<AetherChunk?>()), Times.Never);
    }

    [Test]
    public async Task StaleNormalChunk_ProcessNotificationCalled_SinkNotCalled()
    {
        var processed = WhenProcessed();
        _pipeline.Start(_mockAction.Object, new AetherPipelineOptions { NormalStaleMs = 100 });

        var chunk = new AetherChunk(ChargeState: AetherChargeState.Stable, Presence: ManifestationState.Present)
            { LastWhisperUtc = DateTime.UtcNow.AddSeconds(-10) };

        await _pipeline.OfferAsync(chunk);

        await processed.WaitAsync(TimeSpan.FromSeconds(3));
        await Task.Delay(100);

        _mockAction.Verify(a => a.ProcessNotification(chunk), Times.Once);
        _mockAction.Verify(a => a.Sink(It.IsAny<AetherChunk?>()), Times.Never);
    }

    [Test]
    public async Task MixedChunks_AlertAndNormal_BothSinked()
    {
        var sinked = WhenSinked(count: 2);
        _pipeline.Start(_mockAction.Object, GenerousOpts);

        var alertChunk = new AetherChunk(ChargeState: AetherChargeState.Critical, Presence: ManifestationState.Present);
        var normalChunk = new AetherChunk(ChargeState: AetherChargeState.Full, Presence: ManifestationState.Present);

        await _pipeline.OfferAsync(alertChunk, normalChunk);

        await sinked.WaitAsync(TimeSpan.FromSeconds(5));
        _mockAction.Verify(a => a.Sink(It.IsNotNull<AetherChunk?>()), Times.Exactly(2));
    }
    
    [Test]
    public async Task MixedChunks_ProcessNotification_CalledWithCorrectChargeState()
    {
        var sinked = WhenSinked(count: 2);
        _pipeline.Start(_mockAction.Object, GenerousOpts);

        var alertChunk = new AetherChunk(ChargeState: AetherChargeState.Critical, Presence: ManifestationState.Present);
        var normalChunk = new AetherChunk(ChargeState: AetherChargeState.Full, Presence: ManifestationState.Present);

        await _pipeline.OfferAsync(alertChunk, normalChunk);
        await sinked.WaitAsync(TimeSpan.FromSeconds(5));

        _mockAction.Verify(a => a.ProcessNotification(
            It.Is<AetherChunk>(c => c.ChargeState == AetherChargeState.Critical)), Times.Once);
        _mockAction.Verify(a => a.ProcessNotification(
            It.Is<AetherChunk>(c => c.ChargeState == AetherChargeState.Full)), Times.Once);
    }

    [Test]
    public async Task OfferAsync_WhenPipelineFull_BackpressuresCallerUntilDrained()
    {
        // Block inside ProcessNotification (the first transform in the alert chain).
        // This causes alertBlock to stall, filling up its input queue and then the BufferBlock,
        // so SendAsync for later chunks returns a pending Task — that's backpressure.
        var processEntered = new SemaphoreSlim(0);
        var gate = new ManualResetEventSlim(false);

        _mockAction.Setup(a => a.ProcessNotification(It.IsAny<AetherChunk>()))
            .Returns<AetherChunk>(c =>
            {
                processEntered.Release(); // signal that we're inside the block
                gate.Wait();             // hold the thread until released
                return c;
            });

        var opts = new AetherPipelineOptions { BoundedCapacity = 1, AlertStaleMs = 5000, NormalStaleMs = 5000 };
        _pipeline.Start(_mockAction.Object, opts);

        var chunks = Enumerable.Range(0, 10)
            .Select(_ => new AetherChunk(ChargeState: AetherChargeState.Critical, Presence: ManifestationState.Present))
            .ToArray();

        var offerTask = _pipeline.OfferAsync(chunks);

        // Wait until the first chunk is confirmed stuck inside ProcessNotification.
        // At this point alertBlock's queue (cap=1) + BufferBlock (cap=1) are filling up,
        // leaving chunks 4-10 as pending SendAsync tasks.
        await processEntered.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.That(offerTask.IsCompleted, Is.False, "OfferAsync should be suspended while pipeline is full");

        gate.Set(); // unblock — pipeline drains, all pending SendAsync tasks resolve
        await offerTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(offerTask.IsCompletedSuccessfully, Is.True);
    }
}
