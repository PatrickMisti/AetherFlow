namespace AetherFlow.SystemTests;

public static class HoconConfig
{
    public static string GetConfig()
    {
        return """
               akka {
                   actor {
                       provider = cluster
                   }
                   remote {
                       dot-netty.tcp {
                           hostname = localhost
                           port = 8099
                       }
                   }
                   cluster {
                       seed-nodes = [
                           "akka.tcp://AetherFlowCluster@localhost:8091"
                       ]
                       roles = ["aether-engine-test"]
                   }
               }
               """;
    }
}