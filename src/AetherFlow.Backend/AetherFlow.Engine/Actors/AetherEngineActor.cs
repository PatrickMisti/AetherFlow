using Akka.Actor;
using Akka.Event;

namespace AetherFlow.Engine.Actors;

public class AetherEngineActor : ReceiveActor
{
    private readonly string _entityId;
    
    public  AetherEngineActor(string entityId)
    {
        _entityId = entityId;
        
        Receive<Test>(msg =>
        {
            Context.GetLogger().Info($"Received message: {msg.Message} in actor: {_entityId}");
        });
    }
}