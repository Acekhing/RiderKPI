using KpiApi.Infrastructure;
using KpiApi.Models;
using Microsoft.Extensions.Options;

namespace KpiApi.Services;

public sealed class GpsPingsBackgroundService : BackgroundService
{
    private readonly ILogger<GpsPingsBackgroundService> _logger;
    private readonly GpsPingsProducerSettings _settings;
    private readonly GpsPingsClickHouseWriter _writer;
    private static readonly Random Rng = new();

    public GpsPingsBackgroundService(
        ILogger<GpsPingsBackgroundService> logger,
        IOptions<GpsPingsProducerSettings> settings,
        GpsPingsClickHouseWriter writer)
    {
        _logger = logger;
        _settings = settings.Value;
        _writer = writer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_settings.Enabled)
        {
            _logger.LogInformation("GpsPingsBackgroundService disabled (set GpsPingsProducer:Enabled=true to activate)");
            return;
        }

        await Task.Yield();

        var riders = CreateRiders();
        var buffer = new List<RiderGpsPing>(_settings.BatchSize);

        _logger.LogInformation(
            "GpsPingsBackgroundService started — Riders={Count}, TargetRate={Rate} msg/s, BatchSize={Batch}",
            riders.Length, _settings.TargetRate, _settings.BatchSize);

        long sent = 0;
        var start = DateTime.UtcNow;
        var intervalMs = 1000.0 / _settings.TargetRate;
        var lastFlush = DateTime.UtcNow;

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var cycleStart = DateTime.UtcNow;

                var rider = riders[Rng.Next(riders.Length)];
                var ping = GeneratePing(rider);
                buffer.Add(ping);
                sent++;

                var shouldFlush = buffer.Count >= _settings.BatchSize
                    || (DateTime.UtcNow - lastFlush).TotalMilliseconds >= _settings.FlushIntervalMs;

                if (shouldFlush)
                {
                    await FlushAsync(buffer, stoppingToken);
                    lastFlush = DateTime.UtcNow;

                    var elapsed = (DateTime.UtcNow - start).TotalSeconds;
                    _logger.LogInformation(
                        "[GpsPings] Flushed {Count} pings — total sent: {Total:N0} ({Rate:F0} msg/s)",
                        buffer.Count, sent, sent / Math.Max(elapsed, 1));

                    buffer.Clear();
                }

                var sleepMs = intervalMs - (DateTime.UtcNow - cycleStart).TotalMilliseconds;
                if (sleepMs > 1)
                    await Task.Delay(TimeSpan.FromMilliseconds(sleepMs), stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            if (buffer.Count > 0)
                await FlushAsync(buffer, CancellationToken.None);

            _logger.LogInformation("GpsPingsBackgroundService stopped — total sent: {Count:N0}", sent);
        }
    }

    private async Task FlushAsync(List<RiderGpsPing> buffer, CancellationToken ct)
    {
        try
        {
            await _writer.BulkInsertAsync(buffer, ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to flush {Count} GPS pings to ClickHouse", buffer.Count);
        }
    }

    private SimulatedRider[] CreateRiders()
    {
        var riders = new SimulatedRider[_settings.RiderCount];
        for (var i = 0; i < riders.Length; i++)
        {
            var zoneIdx = Rng.Next(_settings.Zones.Length);
            var center = _settings.ZoneCenters[zoneIdx];

            riders[i] = new SimulatedRider
            {
                RiderId = $"R{i + 1:D4}",
                RiderPhone = $"233{Rng.Next(200_000_000, 600_000_000)}",
                StationId = _settings.Stations[Rng.Next(_settings.Stations.Length)],
                ZoneId = _settings.Zones[zoneIdx],
                Lat = center[0] + (Rng.NextDouble() * 0.02 - 0.01),
                Lon = center[1] + (Rng.NextDouble() * 0.02 - 0.01)
            };
        }

        return riders;
    }

    private static RiderGpsPing GeneratePing(SimulatedRider rider)
    {
        rider.Lat += Rng.NextDouble() * 0.0004 - 0.0002;
        rider.Lon += Rng.NextDouble() * 0.0004 - 0.0002;

        return new RiderGpsPing
        {
            Id = Guid.NewGuid(),
            EventId = Guid.NewGuid(),
            RiderId = rider.RiderId,
            Lat = Math.Round(rider.Lat, 6),
            Lon = Math.Round(rider.Lon, 6),
            RiderPhone = rider.RiderPhone,
            StationId = rider.StationId,
            ZoneId = rider.ZoneId,
            Timestamp = DateTime.UtcNow
        };
    }

    private sealed class SimulatedRider
    {
        public string RiderId { get; init; } = string.Empty;
        public string RiderPhone { get; init; } = string.Empty;
        public string StationId { get; init; } = string.Empty;
        public string ZoneId { get; init; } = string.Empty;
        public double Lat { get; set; }
        public double Lon { get; set; }
    }
}
