namespace KpiApi.Models;

public sealed class ClickHouseSettings
{
    public string ConnectionString { get; set; } = string.Empty;
}

/// <summary>
/// Tunable thresholds for KPI queries — all configurable via appsettings.json.
/// </summary>
public sealed class KpiSettings
{
    public int RealtimeWindowMinutes { get; set; } = 5;
    public int TrendWindowMinutes { get; set; } = 60;
    public int HistoricalWindowHours { get; set; } = 24;
    public int IdleDwellThresholdSeconds { get; set; } = 300;
    public int IdleMovementThresholdMeters { get; set; } = 50;
    public int UtilizationWindowMinutes { get; set; } = 30;
    public int RepositionGapThreshold { get; set; } = 2;
    public int TopZonesLimit { get; set; } = 10;
}

public sealed class CorsSettings
{
    public string[] AllowedOrigins { get; set; } = [];
}
