using KpiApi.Models;
using KpiApi.Repositories;

namespace KpiApi.Services;

/// <summary>
/// Thin service layer — delegates to the repository.  Exists so controllers
/// stay free of direct infrastructure dependencies and to provide a seam for
/// caching / decoration later (e.g. in-memory cache with 5-second TTL).
/// </summary>
public sealed class KpiService : IKpiService
{
    private readonly IKpiRepository _repo;
    private readonly ILogger<KpiService> _log;

    public KpiService(IKpiRepository repo, ILogger<KpiService> log)
    {
        _repo = repo;
        _log = log;
    }

    public async Task<IReadOnlyList<SupplyGapDto>> GetSupplyGapAsync(CancellationToken ct)
    {
        _log.LogDebug("Fetching supply gap KPI");
        return await _repo.GetSupplyGapAsync(ct);
    }

    public async Task<IReadOnlyList<TopZoneDto>> GetTopZonesByPressureAsync(CancellationToken ct)
    {
        _log.LogDebug("Fetching top zones by pressure KPI");
        return await _repo.GetTopZonesByPressureAsync(ct);
    }

    public async Task<IReadOnlyList<IdleRiderDto>> GetIdleRidersAsync(CancellationToken ct)
    {
        _log.LogDebug("Fetching idle riders KPI");
        return await _repo.GetIdleRidersAsync(ct);
    }

    public async Task<IReadOnlyList<RiderUtilizationDto>> GetRiderUtilizationAsync(CancellationToken ct)
    {
        _log.LogDebug("Fetching rider utilization KPI");
        return await _repo.GetRiderUtilizationAsync(ct);
    }

    public async Task<IReadOnlyList<OrdersTrendDto>> GetOrdersTrendAsync(CancellationToken ct)
    {
        _log.LogDebug("Fetching orders trend KPI");
        return await _repo.GetOrdersTrendAsync(ct);
    }

    public async Task<IReadOnlyList<PeakGapDto>> GetPeakGapAsync(CancellationToken ct)
    {
        _log.LogDebug("Fetching peak gap KPI");
        return await _repo.GetPeakGapAsync(ct);
    }

    public async Task<IReadOnlyList<FulfillmentRiskDto>> GetFulfillmentRiskAsync(CancellationToken ct)
    {
        _log.LogDebug("Fetching fulfillment risk KPI");
        return await _repo.GetFulfillmentRiskAsync(ct);
    }

    public async Task<IReadOnlyList<RepositionDto>> GetRepositionRecommendationsAsync(CancellationToken ct)
    {
        _log.LogDebug("Fetching reposition recommendations KPI");
        return await _repo.GetRepositionRecommendationsAsync(ct);
    }

    public async Task<IReadOnlyList<RiderPositionDto>> GetRiderPositionsAsync(CancellationToken ct)
    {
        _log.LogDebug("Fetching rider positions for heatmap");
        return await _repo.GetRiderPositionsAsync(ct);
    }

    public async Task<IReadOnlyList<SurgePredictionDto>> GetSurgePredictionAsync(CancellationToken ct)
    {
        _log.LogDebug("Fetching surge prediction");
        return await _repo.GetSurgePredictionAsync(ct);
    }

    public async Task<IReadOnlyList<string>> GetRiderListAsync(CancellationToken ct)
    {
        _log.LogDebug("Fetching rider list");
        return await _repo.GetRiderListAsync(ct);
    }

    public async Task<IReadOnlyList<RiderRoutePointDto>> GetRiderRouteAsync(string riderId, DateTime? from, DateTime? to, CancellationToken ct)
    {
        _log.LogDebug("Fetching route for rider {RiderId} ({From} → {To})", riderId, from, to);
        return await _repo.GetRiderRouteAsync(riderId, from, to, ct);
    }
}
