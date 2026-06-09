using AetherFlow.Backend.AppHost;
using Microsoft.Extensions.Configuration;

var builder = DistributedApplication.CreateBuilder(args);

const string akkaActorSystemNameDefault = "AetherFlowCluster";
var akkaSystemName = builder.Configuration["ActorSystem"] ?? akkaActorSystemNameDefault;
var seedNodes = builder.Configuration.GetSection("SeedNodes").Get<string[]>() ?? Array.Empty<string>();

builder.AddProject<Projects.AetherFlow_Engine>("aetherflow-engine-1")
    .WithSeedNodes(seedNodes)
    .WithActorSystemName(akkaSystemName)
    .WithRemotePort("8091");

builder.AddProject<Projects.AetherFlow_Engine>("aetherflow-engine-2")
    .WithSeedNodes(seedNodes)
    .WithActorSystemName(akkaSystemName)
    .WithRemotePort("8092");

builder.AddProject<Projects.AetherFlow_Ingestion>("aetherflow-ingestion")
    .WithSeedNodes(seedNodes)
    .WithActorSystemName(akkaSystemName);

builder.Build().Run();
