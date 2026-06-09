namespace AetherFlow.Backend.AppHost;

public static class ResourceBuilderExtension
{
    extension (IResourceBuilder<ProjectResource> builder)
    {
        public IResourceBuilder<ProjectResource> WithSeedNodes(string[] seedNodes)
        {
            for (int i = 0; i < seedNodes.Length; i++)
                builder.WithEnvironment($"Akka__Cluster__SeedNodes__{i}", seedNodes[i]);

            return builder;
        }
        
        public IResourceBuilder<ProjectResource> WithActorSystemName(string actorSystemName)
        {
            return builder.WithEnvironment("Akka__ActorSystemName", actorSystemName);
        }
        
        public IResourceBuilder<ProjectResource> WithRemotePort(string port)
        {
            return builder.WithEnvironment("Akka__Remote__Port", port);
        }
    }
}