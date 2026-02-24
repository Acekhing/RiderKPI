using KpiApi.Models;

namespace KpiApi.Repositories;

public interface IKpiRepository
{
    Task<IReadOnlyList<SupplyGapDto>> GetSupplyGapAsync(CancellationToken ct);
    Task<IReadOnlyList<TopZoneDto>> GetTopZonesByPressureAsync(CancellationToken ct);
    Task<IReadOnlyList<IdleRiderDto>> GetIdleRidersAsync(CancellationToken ct);
    Task<IReadOnlyList<RiderUtilizationDto>> GetRiderUtilizationAsync(CancellationToken ct);
    Task<IReadOnlyList<OrdersTrendDto>> GetOrdersTrendAsync(CancellationToken ct);
    Task<IReadOnlyList<PeakGapDto>> GetPeakGapAsync(CancellationToken ct);
    Task<IReadOnlyList<FulfillmentRiskDto>> GetFulfillmentRiskAsync(CancellationToken ct);
    Task<IReadOnlyList<RepositionDto>> GetRepositionRecommendationsAsync(CancellationToken ct);
    Task<IReadOnlyList<RiderPositionDto>> GetRiderPositionsAsync(CancellationToken ct);
    Task<IReadOnlyList<SurgePredictionDto>> GetSurgePredictionAsync(CancellationToken ct);
    Task<IReadOnlyList<string>> GetRiderListAsync(CancellationToken ct);
    Task<IReadOnlyList<RiderRoutePointDto>> GetRiderRouteAsync(string riderId, DateTime? from, DateTime? to, CancellationToken ct);
}
