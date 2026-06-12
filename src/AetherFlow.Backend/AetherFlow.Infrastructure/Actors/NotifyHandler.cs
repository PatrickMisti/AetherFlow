using Akka.Actor;
using Akka.Event;

namespace AetherFlow.Infrastructure.Actors;

public class NotifyHandler : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    
    // Todo maybe need IRequiredActor of shardRegion
    public NotifyHandler()
    {
        ReceiveAny(msg => _log.Debug("Received notification: {msg}", msg));
        // Todo receive all possible notifications
        // Todo maybe need to forward some notifications to shardRegion or ask coordinator
        // Todo add grpc 
        // Todo maybe need external service to communicate if input or output not work
        //
        // Todo tests check
        // Todo SystemTests maybe move to monitoring tool
        // Review: focus -> notifications -> monitoring tool -> f# app 
    }
}