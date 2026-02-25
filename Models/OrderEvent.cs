namespace KpiApi.Models;

public sealed class OrderEvent
{
    public Guid OrderId { get; set; }
    public string ZoneId { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
}
