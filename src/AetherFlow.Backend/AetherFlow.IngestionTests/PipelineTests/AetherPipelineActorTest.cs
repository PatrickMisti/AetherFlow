using AetherFlow.Domain.Domains;
using AetherFlow.Infrastructure.Actors;
using AetherFlow.Ingestion.Actors;
using AetherFlow.Shared.AetherInterfaces;
using AetherFlow.Shared.Messages.Ingestion;
using AetherFlow.Shared.Messages.Notifications;
using AetherFlow.Shared.Messages.ShardRegion;
using AetherFlow.TestSupport;
using Akka.Actor;
using Akka.Cluster;
using Akka.TestKit;
using Akka.TestKit.NUnit;

namespace AetherFlow.IngestionTests.PipelineTests;

/// <summary>
/// Tests for <see cref="AetherPipelineActor"/>. The actor subscribes to cluster member events in
/// <c>PreStart</c>, so the fixture runs on a cluster-enabled TestKit. The node is only joined in the
/// test that needs the shard connection to flip on (the actor must observe its own MemberUp, which
/// only happens when it subscribes *before* the node goes Up).
/// </summary>
[TestFixture]
public class AetherPipelineActorTest : TestKit
{
    private const string Role = TestKitConfigs.DefaultRole;

    public AetherPipelineActorTest() : base(TestKitConfigs.Cluster(Role)) { }

    private TestProbe _shardProxy = null!;
    private TestProbe _notifier = null!;

    [SetUp]
    public void SetUp()
    {
        _shardProxy = CreateTestProbe("shard-proxy");
        _notifier = CreateTestProbe("notifier");
    }

    private IActorRef CreateActor(string name) =>
        Sys.ActorOf(Props.Create(() => new AetherPipelineActor(
            RequiredActorMock.For<IAetherShardProxyMarker>(_shardProxy.Ref),
            RequiredActorMock.For<NotifyHandler>(_notifier.Ref),
            Role)), name);

    [Test]
    public void Status_BeforeStart_ReturnsNotRunning()
    {
        var actor = CreateActor("pipe-status-before");

        actor.Tell(PipelineStatusRequest.Instance);

        var resp = ExpectMsg<PipelineStatusResponse>(TimeSpan.FromSeconds(3));
        Assert.That(resp.IsRunning, Is.False);
    }

    [Test]
    public void StartPipeline_ThenStatus_ReturnsRunning()
    {
        var actor = CreateActor("pipe-status-after");

        actor.Tell(StartPipelineMessage.Instance);

        AwaitAssert(() =>
        {
            actor.Tell(PipelineStatusRequest.Instance);
            Assert.That(ExpectMsg<PipelineStatusResponse>().IsRunning, Is.True);
        }, TimeSpan.FromSeconds(3));
    }

    [Test]
    public void StopPipeline_StopsActor()
    {
        var actor = CreateActor("pipe-stop");
        actor.Tell(StartPipelineMessage.Instance);

        Watch(actor);
        actor.Tell(StopPipelineMessage.Instance);

        ExpectTerminated(actor, TimeSpan.FromSeconds(5));
    }

    [Test]
    public void OfferInvalidChunk_AfterStart_NotifiesAnomaly()
    {
        var actor = CreateActor("pipe-anomaly");
        actor.Tell(StartPipelineMessage.Instance);

        // Default-kind chunk routes as an alert (Critical + Present) but fails IsValid/IsValidValue,
        // so the pipeline action raises an anomaly notification.
        actor.Tell(new OfferChunksMessage(
            new AetherChunk(ChargeState: AetherChargeState.Critical, Presence: ManifestationState.Present)));

        _notifier.ExpectMsg<ChunkAnomalyNotification>(TimeSpan.FromSeconds(5));
    }

    [Test]
    public void OfferValidAlertChunk_WhenConnectedToShard_ForwardsToShardProxy()
    {
        var cluster = Cluster.Get(Sys);
        cluster.Join(cluster.SelfAddress);
        AwaitCondition(() => cluster.SelfMember.Status == MemberStatus.Up, TimeSpan.FromSeconds(15));

        var actor = CreateActor("pipe-forward");
        actor.Tell(StartPipelineMessage.Instance);

        // Deliver the (real) self member-up the actor listens for — its role matches the configured
        // shardRegionRole, so this flips IsConnectedToShard on. Done explicitly to avoid the race
        // between PreStart's cluster subscription and the node reaching Up.
        actor.Tell(new ClusterEvent.MemberUp(cluster.SelfMember));

        AwaitAssert(() =>
        {
            actor.Tell(new OfferChunksMessage(AetherChunkFactory.Valid(AetherChargeState.Critical)));
            _shardProxy.ExpectMsg<ChunkShardMessage>(TimeSpan.FromMilliseconds(500));
        }, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(500));
    }
}
