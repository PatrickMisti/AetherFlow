using AetherFlow.Shared.Messages.Ingestion;
using Akka.Actor;
using Akka.TestKit;

namespace AetherFlow.TestSupport;

/// <summary>
/// Test-probe helpers for actors that <c>Ask</c> the pipeline for its status.
/// </summary>
public static class PipelineProbeExtensions
{
    /// <summary>
    /// Creates a probe that auto-replies to <see cref="PipelineStatusRequest"/> with a
    /// <see cref="PipelineStatusResponse"/> carrying <paramref name="isRunning"/>, while still
    /// recording every message it receives.
    /// </summary>
    public static TestProbe CreatePipelineProbe(this TestKitBase kit, bool isRunning = false)
    {
        var probe = kit.CreateTestProbe();
        probe.SetAutoPilot(new DelegateAutoPilot((sender, message) =>
        {
            if (message is PipelineStatusRequest)
                sender.Tell(new PipelineStatusResponse(isRunning));
            return AutoPilot.KeepRunning;
        }));
        return probe;
    }
}
