using AetherFlow.Domain.Domains;
using Akka.Actor;

namespace AetherFlow.Shared.Messages.ShardRegion;

public record ChunkShardMessage(string EntityId, AetherChunk Chunk, IActorRef ActorRef) 
    : BaseShardMessage(EntityId, ActorRef);