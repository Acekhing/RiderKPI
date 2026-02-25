namespace KpiApi.Models;

public sealed class RiderGpsPing
{
    public Guid Id { get; set; }
    public Guid EventId { get; set; }
    public string RiderId { get; set; } = string.Empty;
    public double Lat { get; set; }
    public double Lon { get; set; }
    public string RiderPhone { get; set; } = string.Empty;
    public string StationId { get; set; } = string.Empty;
    public string ZoneId { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
}
