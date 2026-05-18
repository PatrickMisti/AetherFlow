var builder = DistributedApplication.CreateBuilder(args);

const string akkaActorSystemNameDefault = "AetherFlowCluster";
var akkaSystemName = builder.Configuration["Akka:ActorSystemName"] ?? akkaActorSystemNameDefault;
builder.AddProject<Projects.AetherFlow_Engine>("aetherflow-engine")
    .WithEnvironment("Akka__ActorSystemName", akkaSystemName);

builder.Build().Run();
