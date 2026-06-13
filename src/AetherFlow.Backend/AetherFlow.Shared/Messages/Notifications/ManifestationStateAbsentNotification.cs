namespace AetherFlow.Shared.Messages.Notifications;

public class ManifestationStateAbsentNotification : INotification
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public DateTime CreatedUtc { get; init; } = DateTime.UtcNow;
    private ManifestationStateAbsentNotification() { }

    public static ManifestationStateAbsentNotification Instance => new();
}