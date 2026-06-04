namespace AetherFlow.Shared.Config;

public class AkkaDbSettings
{
    public string ConnectionString { get; set; } = "Data Source=data.db";
    public string ProviderName { get; set; } = "SQLite.MS";
}