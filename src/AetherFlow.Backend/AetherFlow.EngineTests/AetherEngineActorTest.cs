using AetherFlow.Domain.Domains;
using AetherFlow.Domain.EngineDomains;
using AetherFlow.Engine.Actors;
using AetherFlow.Engine.Messages;
using AetherFlow.Infrastructure.Actors;
using AetherFlow.Shared.Messages.Notifications;
using AetherFlow.Shared.Messages.ShardRegion;
using AetherFlow.TestSupport;
using Akka.Actor;
using Akka.Cluster;
using Akka.TestKit;
using Akka.TestKit.NUnit;

namespace AetherFlow.EngineTests;

/// <summary>
/// Tests for <see cref="AetherEngineActor"/>. It is a persistent actor that subscribes to the
/// distributed pub-sub "capacity" topic in <c>PreStart</c> and only initialises after the
/// <c>SubscribeAck</c>, so the fixture runs on a single-node cluster with persistence. Behaviour is
/// observed through the notification side-effects sent to the (probe-backed) notify handler.
/// </summary>
[TestFixture]
public class AetherEngineActorTest : TestKit
{
    public AetherEngineActorTest() : base(TestKitConfigs.ClusterWithPersistence()) { }

    private TestProbe _notifier = null!;

    [SetUp]
    public void SetUp()
    {
        var cluster = Cluster.Get(Sys);
        cluster.Join(cluster.SelfAddress);
        AwaitCondition(() => cluster.SelfMember.Status == MemberStatus.Up, TimeSpan.FromSeconds(15));

        _notifier = CreateTestProbe("notifier");
    }

    private IActorRef CreateEngine(string name) =>
        Sys.ActorOf(Props.Create(() => new AetherEngineActor(
            RequiredActorMock.For<NotifyHandler>(_notifier.Ref), null)), name);

    private ChunkShardMessage Msg(AetherChunk chunk, string entityId = "entity-1") =>
        new(entityId, chunk, TestActor);

    [Test]
    public void ChunkShardMessage_NotifiesCalculationLatency()
    {
        var engine = CreateEngine("engine-latency");

        engine.Tell(Msg(AetherChunkFactory.Valid(AetherChargeState.Stable)));

        _notifier.FishForMessage<CalculationLatencyNotification>(_ => true, TimeSpan.FromSeconds(10));
    }

    [Test]
    public void ChunkShardMessage_NotifiesChargingLevel_WithChunkChargeState()
    {
        var engine = CreateEngine("engine-charging");

        engine.Tell(Msg(AetherChunkFactory.Valid(AetherChargeState.Critical)));

        var notification = _notifier.FishForMessage<ChargingLevelNotification>(
            m => m.Level == AetherChargeState.Critical, TimeSpan.FromSeconds(10));
        Assert.That(notification.Level, Is.EqualTo(AetherChargeState.Critical));
    }

    [Test]
    public void AbsentChunk_NotifiesManifestationStateAbsent()
    {
        var engine = CreateEngine("engine-absent");

        engine.Tell(Msg(AetherChunkFactory.Valid(AetherChargeState.Stable, ManifestationState.Absent)));

        _notifier.FishForMessage<ManifestationStateAbsentNotification>(_ => true, TimeSpan.FromSeconds(10));
    }

    [Test]
    public void CapacityChange_KeepsActorResponsive()
    {
        var engine = CreateEngine("engine-capacity");

        // Triggers a SaveSnapshot internally; the actor must survive it and keep processing.
        engine.Tell(new ChunkCapacityChangeNotification(5, TestActor));
        engine.Tell(Msg(AetherChunkFactory.Valid(AetherChargeState.Full)));

        _notifier.FishForMessage<CalculationLatencyNotification>(_ => true, TimeSpan.FromSeconds(10));
    }

    [Test]
    public void MultipleChunks_AllProduceLatencyNotifications()
    {
        var engine = CreateEngine("engine-multi");

        for (var i = 0; i < 3; i++)
            engine.Tell(Msg(AetherChunkFactory.Valid(AetherChargeState.Stable), $"entity-{i}"));

        for (var i = 0; i < 3; i++)
            _notifier.FishForMessage<CalculationLatencyNotification>(_ => true, TimeSpan.FromSeconds(10));
    }
    
    [Test]
    public async Task CapacityCheck_AddItemsToQueue()
    {
        var engine = CreateEngine("engine-check");
        int capacity = 5;
        
        // Wait until the actor has finished its subscribe/initialization phase. Before that
        // messages like SaveEngineValueCommand are being stashed and Ask/Monitoring requests
        // won't be handled.
        AwaitCondition(() =>
        {
            try
            {
                // small timeout: if actor hasn't initialized yet this will throw/timeout quickly
                engine.Ask<MonitoringAetherChunkMessageResponse>(
                    MonitoringAetherChunkMessageRequest.Instance(),
                    TimeSpan.FromMilliseconds(200)).GetAwaiter().GetResult();
                return true;
            }
            catch
            {
                return false;
            }
        }, TimeSpan.FromSeconds(5));

        // Now the actor should be initialized and will process incoming SaveEngineValueCommand messages.
        engine.Tell(new ChunkCapacityChangeNotification(capacity, TestActor));
        for (int i = 0; i < 10; i++)
            engine.Tell(new SaveEngineValueCommand(AetherChunkFactory.Valid(AetherChargeState.Full).ToEngineValue()));

        // Ask for the monitoring response (allow some time for processing)
        var msg = await engine.Ask<MonitoringAetherChunkMessageResponse>(
            MonitoringAetherChunkMessageRequest.Instance(),
            TimeSpan.FromSeconds(5));
        
        Assert.AreEqual(capacity, msg.Values.Count);
    }
}
