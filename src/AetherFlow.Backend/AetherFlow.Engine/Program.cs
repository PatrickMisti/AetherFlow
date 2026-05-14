using AetherFlow.Backend.ServiceDefaults;
using AetherFlow.Engine;
using AetherFlow.Engine.Actors;
using AetherFlow.Infrastructure.AetherShardRegion;
using AetherFlow.Shared.AetherInterfaces;
using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding;
using Akka.Hosting;
using Akka.Logger.Serilog;
using Akka.Remote.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.AddServiceDefaults();

var akkaActorSystemName = builder.Configuration["Akka:ActorSystemName"] ?? throw new InvalidOperationException("Akka Actor System Name is not configured.");

builder.Services.AddAkka(akkaActorSystemName, config =>
{
    config.ConfigureLoggers(opt =>
    {
        opt.ClearLoggers();
        opt.AddSerilogLogging();
    });

    config.WithRemoting(opt =>
    {
        opt.HostName = "localhost";
        opt.Port = 9090;
    });
    config.WithClustering(new()
    {
        Roles = ["aether-engine"],
        SeedNodes = [$"akka.tcp://{akkaActorSystemName}@localhost:9090"]
    });

    config.WithShardRegion<IAetherShardMarker>(
        typeName: "aether-engine",
        entityPropsFactory: (entityId) =>
        {
            var props = Props.Create(() => new AetherEngineActor(entityId));
            return props;
        },
        messageExtractor: CustomMessageExtractor.Create(),
        /*HashCodeMessageExtractor.Create(maxNumberOfShards: 100,
            entityIdExtractor: msg => msg switch
            {
                _ => null
            }),*/
        new()
        {
            StateStoreMode = StateStoreMode.DData,
            Role = "aether-engine",
            // RememberEntities = true, // not in combination with PassivateIdleEntityAfter
            PassivateIdleEntityAfter = TimeSpan.FromMinutes(2)
        });
});


builder.Services.AddHostedService<Worker>();



var host = builder.Build();
host.Run();