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

public sealed class GpsPingsProducerSettings
{
    public bool Enabled { get; set; }
    public int RiderCount { get; set; } = 50;
    public int TargetRate { get; set; } = 500;
    public int BatchSize { get; set; } = 1000;
    public int FlushIntervalMs { get; set; } = 2000;
    public string[] Zones { get; set; } =
        ["Z-ACCRA-CENTRAL", "Z-EAST-LEGON", "Z-OOSOU", "Z-TEMA", "Z-LAPAZ", "Z-MADINA"];
    public string[] Stations { get; set; } =
        ["S-HQ", "S-EAST", "S-WEST", "S-NORTH", "S-SOUTH", "S-TEMA"];
    public double[][] ZoneCenters { get; set; } =
    [
        [5.6037, -0.1870],
        [5.6350, -0.1530],
        [5.6365, -0.0166],
        [5.6698, -0.0166],
        [5.6100, -0.2500],
        [5.6700, -0.1700]
    ];
}

public sealed class OrdersProducerSettings
{
    public bool Enabled { get; set; }
    public int MinIntervalMs { get; set; } = 200;
    public int MaxIntervalMs { get; set; } = 500;
    public int BatchSize { get; set; } = 500;
    public int FlushIntervalMs { get; set; } = 2000;
    public double SpikeProbability { get; set; } = 0.3;
    public string[] SpikeZones { get; set; } = ["Z-ACCRA-CENTRAL", "Z-EAST-LEGON"];
    public string[] AllZones { get; set; } =
        ["Z-ACCRA-CENTRAL", "Z-EAST-LEGON", "Z-OOSOU", "Z-TEMA", "Z-LAPAZ", "Z-MADINA"];
}
