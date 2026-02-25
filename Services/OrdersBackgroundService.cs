using KpiApi.Infrastructure;
using KpiApi.Models;
using Microsoft.Extensions.Options;

namespace KpiApi.Services;

public sealed class OrdersBackgroundService : BackgroundService
{
    private readonly ILogger<OrdersBackgroundService> _logger;
    private readonly OrdersProducerSettings _settings;
    private readonly OrdersClickHouseWriter _writer;
    private static readonly Random Rng = new();

    public OrdersBackgroundService(
        ILogger<OrdersBackgroundService> logger,
        IOptions<OrdersProducerSettings> settings,
        OrdersClickHouseWriter writer)
    {
        _logger = logger;
        _settings = settings.Value;
        _writer = writer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_settings.Enabled)
        {
            _logger.LogInformation("OrdersBackgroundService disabled (set OrdersProducer:Enabled=true to activate)");
            return;
        }

        await Task.Yield();

        var buffer = new List<OrderEvent>(_settings.BatchSize);

        _logger.LogInformation(
            "OrdersBackgroundService started — Interval={Min}-{Max}ms, SpikeProbability={Spike:P0}, BatchSize={Batch}",
            _settings.MinIntervalMs, _settings.MaxIntervalMs, _settings.SpikeProbability, _settings.BatchSize);

        long sent = 0;
        var start = DateTime.UtcNow;
        var lastFlush = DateTime.UtcNow;

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var order = new OrderEvent
                {
                    OrderId = Guid.NewGuid(),
                    ZoneId = PickZone(),
                    CreatedAt = DateTime.UtcNow
                };

                buffer.Add(order);
                sent++;

                var shouldFlush = buffer.Count >= _settings.BatchSize
                    || (DateTime.UtcNow - lastFlush).TotalMilliseconds >= _settings.FlushIntervalMs;

                if (shouldFlush)
                {
                    await FlushAsync(buffer, stoppingToken);
                    lastFlush = DateTime.UtcNow;

                    var elapsed = (DateTime.UtcNow - start).TotalSeconds;
                    _logger.LogInformation(
                        "[Orders] Flushed {Count} orders — total sent: {Total:N0} ({Rate:F0} msg/s)",
                        buffer.Count, sent, sent / Math.Max(elapsed, 1));

                    buffer.Clear();
                }

                var delay = Rng.Next(_settings.MinIntervalMs, _settings.MaxIntervalMs + 1);
                await Task.Delay(delay, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            if (buffer.Count > 0)
                await FlushAsync(buffer, CancellationToken.None);

            _logger.LogInformation("OrdersBackgroundService stopped — total sent: {Count:N0}", sent);
        }
    }

    private async Task FlushAsync(List<OrderEvent> buffer, CancellationToken ct)
    {
        try
        {
            await _writer.BulkInsertAsync(buffer, ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to flush {Count} orders to ClickHouse", buffer.Count);
        }
    }

    private string PickZone()
    {
        if (Rng.NextDouble() < _settings.SpikeProbability && _settings.SpikeZones.Length > 0)
            return _settings.SpikeZones[Rng.Next(_settings.SpikeZones.Length)];

        return _settings.AllZones[Rng.Next(_settings.AllZones.Length)];
    }
}
