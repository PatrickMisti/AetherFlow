using AetherFlow.Shared.Messages.Notifications;
using Akka.Actor;
using Akka.Event;

namespace AetherFlow.Infrastructure.Actors;

public class NotifyHandler : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    
    public NotifyHandler()
    {
        
        // Todo notify worker when time to high create new actor with router and these worker make communication
        // todo interface in worker to connection so it is possible to change anytime (grpc)
        // Engine Solution
        Receive<MonitoringAetherChunkMessageResponse>(msg => _log.Info(msg.ToString()));
        Receive<ManifestationStateAbsentNotification>(msg => _log.Info(msg.ToString()));
        Receive<ChargingLevelNotification>(msg => _log.Info(msg.ToString()));
        Receive<CalculationLatencyNotification>(msg => _log.Info(msg.ToString()));
        
        // Ingestion Solution
        Receive<ChunkAnomalyNotification>(msg => _log.Info(msg.ToString()));

        // Not interested now
        ReceiveAny(msg => _log.Debug("Received notification: {fullname}", msg.GetType().FullName));
        // Todo maybe need to forward some notifications to shardRegion or ask coordinator
        // Todo add grpc 
        // Todo SystemTests maybe move to monitoring tool
        // Review: focus -> monitoring tool -> f# app 
    }
}