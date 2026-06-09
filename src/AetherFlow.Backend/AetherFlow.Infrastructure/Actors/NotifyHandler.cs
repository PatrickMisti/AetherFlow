using Akka.Actor;
using Akka.Event;

namespace AetherFlow.Infrastructure.Actors;

public class NotifyHandler : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    
    public NotifyHandler()
    {
        ReceiveAny(msg => _log.Info("Received notification: {msg}", msg));
    }
}