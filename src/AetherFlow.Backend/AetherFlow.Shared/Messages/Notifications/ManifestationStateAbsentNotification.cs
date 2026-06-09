namespace AetherFlow.Shared.Messages.Notifications;

public class ManifestationStateAbsentNotification : INotification
{
    public Guid Id { get; init; } = Guid.NewGuid();
    private ManifestationStateAbsentNotification() { }

    public static ManifestationStateAbsentNotification Instance => new();
}