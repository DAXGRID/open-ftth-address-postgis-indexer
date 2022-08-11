using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address.Events;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressPostgisProjector;

internal sealed record PostCode(string Code, string Name);
internal sealed record Road(string OfficialId, string Name);

internal sealed record AccessAddress(
    string? OfficialId,
    string MunicipalCode,
    string Status,
    string RoadCode,
    string HouseNumber,
    double EastCoordinate,
    double NorthCoordinate,
    string? TownName,
    string? PlotId,
    Guid RoadId,
    Guid PostCodeId,
    bool Deleted);

internal sealed record UnitAddress(
    Guid AccessAddresssId,
    string Status,
    string? FloorName,
    string? SuitName,
    string? OfficialId,
    bool Deleted);

internal sealed class AddressPostgisProjection : ProjectionBase
{
    public int Count;
    public readonly Dictionary<Guid, PostCode> IdToPostCode = new();
    public readonly Dictionary<Guid, Road> IdToRoad = new();
    public readonly Dictionary<Guid, AccessAddress> IdToAccessAddress = new();
    public readonly Dictionary<Guid, UnitAddress> IdToUnitAddress = new();

    public AddressPostgisProjection(ILogger<AddressPostgisProjection> logger)
    {
        ProjectEventAsync<PostCodeCreated>(ProjectAsync);
        ProjectEventAsync<PostCodeUpdated>(ProjectAsync);
        ProjectEventAsync<PostCodeDeleted>(ProjectAsync);

        ProjectEventAsync<RoadCreated>(ProjectAsync);
        ProjectEventAsync<RoadUpdated>(ProjectAsync);
        ProjectEventAsync<RoadDeleted>(ProjectAsync);

        ProjectEventAsync<AccessAddressCreated>(ProjectAsync);
        ProjectEventAsync<AccessAddressUpdated>(ProjectAsync);
        ProjectEventAsync<AccessAddressDeleted>(ProjectAsync);

        ProjectEventAsync<UnitAddressCreated>(ProjectAsync);
        ProjectEventAsync<UnitAddressUpdated>(ProjectAsync);
        ProjectEventAsync<UnitAddressDeleted>(ProjectAsync);
    }

    private Task ProjectAsync(IEventEnvelope eventEnvelope)
    {
        Count++;

        switch (eventEnvelope.Data)
        {
            case (PostCodeCreated postCodeCreated):
                HandlePostCodeCreated(postCodeCreated);
                break;
            case (PostCodeUpdated postCodeUpdated):
                HandlePostCodeUpdated(postCodeUpdated);
                break;
            case (PostCodeDeleted postCodeDeleted):
                HandlePostCodeDeleted(postCodeDeleted);
                break;
            case (RoadCreated roadCreated):
                HandleRoadCreated(roadCreated);
                break;
            case (RoadUpdated roadUpdated):
                HandleRoadUpdated(roadUpdated);
                break;
            case (RoadDeleted roadDeleted):
                HandleRoadDeleted(roadDeleted);
                break;
            case (AccessAddressCreated accessAddressCreated):
                HandleAccessAddressCreated(accessAddressCreated);
                break;
            case (AccessAddressUpdated accessAddressUpdated):
                HandleAccessAddressUpdated(accessAddressUpdated);
                break;
            case (AccessAddressDeleted accessAddressDeleted):
                HandleAccessAddressDeleted(accessAddressDeleted);
                break;
            case (UnitAddressCreated unitAddressCreated):
                HandleUnitAddressCreated(unitAddressCreated);
                break;
            case (UnitAddressUpdated unitAddressUpdated):
                HandleUnitAddressUpdated(unitAddressUpdated);
                break;
            case (UnitAddressDeleted unitAddressDeleted):
                HandleUnitAddressDeleted(unitAddressDeleted);
                break;
            default:
                throw new ArgumentException(
                    $"Could not handle typeof '{eventEnvelope.Data.GetType().Name}'");
        }

        return Task.CompletedTask;
    }

    private void HandleAccessAddressCreated(AccessAddressCreated accessAddressCreated)
    {
        IdToAccessAddress.Add(
            accessAddressCreated.Id,
            new(OfficialId: accessAddressCreated.OfficialId,
                MunicipalCode: accessAddressCreated.MunicipalCode,
                Status: accessAddressCreated.Status.ToString(),
                RoadCode: accessAddressCreated.RoadCode,
                HouseNumber: accessAddressCreated.HouseNumber,
                NorthCoordinate: accessAddressCreated.NorthCoordinate,
                EastCoordinate: accessAddressCreated.EastCoordinate,
                TownName: accessAddressCreated.TownName,
                PlotId: accessAddressCreated.PlotId,
                RoadId: accessAddressCreated.RoadId,
                PostCodeId: accessAddressCreated.PostCodeId,
                Deleted: false));
    }

    private void HandleAccessAddressUpdated(AccessAddressUpdated accessAddressUpdated)
    {
        var oldAccessAddress = IdToAccessAddress[accessAddressUpdated.Id];
        IdToAccessAddress[accessAddressUpdated.Id] = oldAccessAddress with
        {
            OfficialId = accessAddressUpdated.OfficialId,
            MunicipalCode = accessAddressUpdated.MunicipalCode,
            Status = accessAddressUpdated.Status.ToString(),
            RoadCode = accessAddressUpdated.RoadCode,
            HouseNumber = accessAddressUpdated.HouseNumber,
            NorthCoordinate = accessAddressUpdated.NorthCoordinate,
            EastCoordinate = accessAddressUpdated.EastCoordinate,
            TownName = accessAddressUpdated.TownName,
            PlotId = accessAddressUpdated.PlotId,
            RoadId = accessAddressUpdated.RoadId,
            PostCodeId = accessAddressUpdated.PostCodeId,
        };
    }

    private void HandleAccessAddressDeleted(AccessAddressDeleted accessAddressDeleted)
    {
        var oldAccessAddress = IdToAccessAddress[accessAddressDeleted.Id];
        IdToAccessAddress[accessAddressDeleted.Id] = oldAccessAddress with
        {
            Deleted = true
        };
    }

    private void HandleUnitAddressCreated(UnitAddressCreated unitAddressCreated)
    {
        IdToUnitAddress.Add(
            unitAddressCreated.Id,
            new UnitAddress(
                OfficialId: unitAddressCreated.OfficialId,
                AccessAddresssId: unitAddressCreated.AccessAddressId,
                Status: unitAddressCreated.Status.ToString(),
                FloorName: unitAddressCreated.FloorName,
                SuitName: unitAddressCreated.SuitName,
                Deleted: false
            ));
    }

    private void HandleUnitAddressUpdated(UnitAddressUpdated unitAddressUpdated)
    {
        var unitAddress = IdToUnitAddress[unitAddressUpdated.Id];
        IdToUnitAddress[unitAddressUpdated.Id] = unitAddress with
        {
            AccessAddresssId = unitAddressUpdated.AccessAddressId,
            OfficialId = unitAddressUpdated.OfficialId,
            Status = unitAddressUpdated.Status.ToString(),
            FloorName = unitAddressUpdated.FloorName,
            SuitName = unitAddressUpdated.SuitName,
        };
    }

    private void HandleUnitAddressDeleted(UnitAddressDeleted unitAddressDeleted)
    {
        var unitAddress = IdToUnitAddress[unitAddressDeleted.Id];
        IdToUnitAddress[unitAddressDeleted.Id] = unitAddress with
        {
            Deleted = true
        };
    }

    private void HandlePostCodeCreated(PostCodeCreated postCodeCreated)
    {
        IdToPostCode.Add(
            postCodeCreated.Id,
            new(postCodeCreated.Number, postCodeCreated.Name));
    }

    private void HandlePostCodeUpdated(PostCodeUpdated postCodeUpdated)
    {
        var postCode = IdToPostCode[postCodeUpdated.Id];
        IdToPostCode[postCodeUpdated.Id] = postCode with
        {
            Name = postCodeUpdated.Name
        };
    }

    private void HandlePostCodeDeleted(PostCodeDeleted postCodeDeleted)
    {
        IdToPostCode.Remove(postCodeDeleted.Id);
    }

    private void HandleRoadCreated(RoadCreated roadCreated)
    {
        var road = new Road(roadCreated.OfficialId, roadCreated.Name);
        IdToRoad.Add(roadCreated.Id, road);
    }

    private void HandleRoadUpdated(RoadUpdated roadUpdated)
    {
        var road = IdToRoad[roadUpdated.Id];
        IdToRoad[roadUpdated.Id] = road with
        {
            Name = roadUpdated.Name
        };
    }

    private void HandleRoadDeleted(RoadDeleted roadDeleted)
    {
        IdToRoad.Remove(roadDeleted.Id);
    }
}
