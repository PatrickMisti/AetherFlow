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

    public static string GetShardConfig() => """
                                             akka {
                                                 log-dead-letters = off
                                                 actor {
                                                     provider = cluster
                                                     serializers {
                                                         hyperion = "Akka.Serialization.HyperionSerializer, Akka.Serialization.Hyperion"
                                                     }
                                                     serialization-bindings {
                                                         "System.Object" = hyperion
                                                     }
                                                 }
                                                 remote {
                                                     dot-netty.tcp {
                                                         hostname = localhost
                                                         port = 0
                                                     }
                                                     log-remote-lifecycle-events = off 
                                                 }
                                                 cluster {
                                                     seed-nodes = ["akka.tcp://AetherFlowCluster@localhost:8091"]
                                                     roles = ["aether-engine-test"]
                                                 }
                                                 loglevel = DEBUG
                                                 cluster.sharding {
                                                     verbose-debug-logging = on
                                                 }
                                             }
                                             """;
}