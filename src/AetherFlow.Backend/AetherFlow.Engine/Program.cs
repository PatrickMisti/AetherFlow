using AetherFlow.Engine.Actors;
using AetherFlow.Shared.AetherInterfaces;
using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding;
using Akka.Hosting;
using Akka.Logger.Serilog;
using Akka.Remote.Hosting;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

// var builder = Host.CreateApplicationBuilder(args);

Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .CreateLogger();
builder.Logging.ClearProviders();

var akkaActorSystemName = builder.Configuration["Akka:ActorSystemName"] ?? "AetherFlowIngestion";


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
        messageExtractor: HashCodeMessageExtractor.Create(maxNumberOfShards: 100,
            entityIdExtractor: msg => msg switch
            {
                _ => null
            }),
        new()
        {
            StateStoreMode = StateStoreMode.DData,
            Role = "aether-engine",
            // RememberEntities = true, // not in combination with PassivateIdleEntityAfter
            PassivateIdleEntityAfter = TimeSpan.FromMinutes(2)
        });
});

var host = builder.Build();


host.MapGet("/", (IRequiredActor<IAetherShardMarker> shardRegion) =>
{
    var s = new Test("Hello from the web endpoint!");
    // Example of sending a message to the shard region
    shardRegion.ActorRef.Tell(new ShardingEnvelope("hallo", s));
    return "Message sent to Aether Engine!";
});

host.Run("http://localhost:3000");

public record Test(string Message);