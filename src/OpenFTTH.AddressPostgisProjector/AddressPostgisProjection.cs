using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address.Events;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressPostgisProjector;

internal sealed record PostCode(string Code, string Name, bool Deleted);
internal sealed record Road(string ExternalId, string Name, bool Deleted);

internal sealed record AccessAddress(
    string? ExternalId,
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
    DateTime Created,
    DateTime? Updated,
    bool Deleted);

internal sealed record UnitAddress(
    Guid AccessAddressId,
    string Status,
    string? FloorName,
    string? SuitName,
    string? ExternalId,
    DateTime Created,
    DateTime? Updated,
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
        ProjectEventAsync<PostCodeNameChanged>(ProjectAsync);
        ProjectEventAsync<PostCodeDeleted>(ProjectAsync);

        ProjectEventAsync<RoadCreated>(ProjectAsync);
        ProjectEventAsync<RoadNameChanged>(ProjectAsync);
        ProjectEventAsync<RoadExternalIdChanged>(ProjectAsync);
        ProjectEventAsync<RoadDeleted>(ProjectAsync);

        ProjectEventAsync<AccessAddressCreated>(ProjectAsync);
        ProjectEventAsync<AccessAddressExternalIdChanged>(ProjectAsync);
        ProjectEventAsync<AccessAddressMunicipalCodeChanged>(ProjectAsync);
        ProjectEventAsync<AccessAddressStatusChanged>(ProjectAsync);
        ProjectEventAsync<AccessAddressRoadCodeChanged>(ProjectAsync);
        ProjectEventAsync<AccessAddressHouseNumberChanged>(ProjectAsync);
        ProjectEventAsync<AccessAddressSupplementaryTownNameChanged>(ProjectAsync);
        ProjectEventAsync<AccessAddressPlotIdChanged>(ProjectAsync);
        ProjectEventAsync<AccessAddressRoadIdChanged>(ProjectAsync);
        ProjectEventAsync<AccessAddressCoordinateChanged>(ProjectAsync);
        ProjectEventAsync<AccessAddressDeleted>(ProjectAsync);

        ProjectEventAsync<UnitAddressCreated>(ProjectAsync);
        ProjectEventAsync<UnitAddressExternalIdChanged>(ProjectAsync);
        ProjectEventAsync<UnitAddressAccessAddressIdChanged>(ProjectAsync);
        ProjectEventAsync<UnitAddressStatusChanged>(ProjectAsync);
        ProjectEventAsync<UnitAddressFloorNameChanged>(ProjectAsync);
        ProjectEventAsync<UnitAddressSuiteNameChanged>(ProjectAsync);
        ProjectEventAsync<UnitAddressPendingOfficialChanged>(ProjectAsync);
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
            case (PostCodeNameChanged postCodeNameChanged):
                HandlePostCodeNameChanged(postCodeNameChanged);
                break;
            case (PostCodeDeleted postCodeDeleted):
                HandlePostCodeDeleted(postCodeDeleted);
                break;
            case (RoadCreated roadCreated):
                HandleRoadCreated(roadCreated);
                break;
            case (RoadNameChanged roadNameChanged):
                HandleRoadNameChanged(roadNameChanged);
                break;
            case (RoadExternalIdChanged roadExternalIdChanged):
                HandleRoadExternalIdChanged(roadExternalIdChanged);
                break;
            case (RoadDeleted roadDeleted):
                HandleRoadDeleted(roadDeleted);
                break;
            case (AccessAddressCreated accessAddressCreated):
                HandleAccessAddressCreated(accessAddressCreated);
                break;
            case (AccessAddressExternalIdChanged accessAddressExternalIdChanged):
                HandleAccessAddressExternalIdChanged(accessAddressExternalIdChanged);
                break;
            case (AccessAddressMunicipalCodeChanged accessAddressMunicipalCodeChanged):
                HandleAccessAddressMunicipalCodeChanged(accessAddressMunicipalCodeChanged);
                break;
            case (AccessAddressStatusChanged accessAddressStatusChanged):
                HandleAccessAddressStatusChanged(accessAddressStatusChanged);
                break;
            case (AccessAddressRoadCodeChanged accessAddressRoadCodeChanged):
                HandleAccessAddressRoadCodeChanged(accessAddressRoadCodeChanged);
                break;
            case (AccessAddressHouseNumberChanged accessAddressHouseNumberChanged):
                HandleAccessAddressHouseNumberChanged(accessAddressHouseNumberChanged);
                break;
            case (AccessAddressCoordinateChanged accessAddressCoordinateChanged):
                HandleAccessAddressCoordinateChanged(
                    accessAddressCoordinateChanged); break;
            case (AccessAddressSupplementaryTownNameChanged accessAddressSupplementaryTownNameChanged):
                HandleAccessAddressSupplementaryTownNameChanged(accessAddressSupplementaryTownNameChanged);
                break;
            case (AccessAddressPlotIdChanged handleAccessAddressPlotIdChanged):
                HandleAccessAddressPlotIdChanged(handleAccessAddressPlotIdChanged);
                break;
            case (AccessAddressRoadIdChanged handleAccessAddressRoadIdChanged):
                HandleAccessAddressRoadIdChanged(handleAccessAddressRoadIdChanged);
                break;
            case (AccessAddressDeleted accessAddressDeleted):
                HandleAccessAddressDeleted(accessAddressDeleted);
                break;
            case (UnitAddressCreated unitAddressCreated):
                HandleUnitAddressCreated(unitAddressCreated);
                break;
            case (UnitAddressExternalIdChanged unitAddressExternalIdChanged):
                HandleUnitAddressExternalIdChanged(unitAddressExternalIdChanged);
                break;
            case (UnitAddressAccessAddressIdChanged unitAddressAccessAddressIdChanged):
                HandleUnitAddressAccessAddressIdChanged(unitAddressAccessAddressIdChanged);
                break;
            case (UnitAddressStatusChanged unitAddressStatusChanged):
                HandleUnitAddressStatusChanged(unitAddressStatusChanged);
                break;
            case (UnitAddressFloorNameChanged unitAddressFloorNameChanged):
                HandleUnitAddressFloorNameChanged(unitAddressFloorNameChanged);
                break;
            case (UnitAddressSuiteNameChanged unitAddressSuiteNameChanged):
                HandleUnitAddressSuiteNameChanged(unitAddressSuiteNameChanged);
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
            new(ExternalId: accessAddressCreated.ExternalId,
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
                Created: accessAddressCreated.ExternalCreatedDate ?? throw new ArgumentException("External created date should never be null."),
                Updated: accessAddressCreated.ExternalUpdatedDate,
                Deleted: false));
    }

    private void HandleAccessAddressExternalIdChanged(AccessAddressExternalIdChanged changedEvent)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            ExternalId = changedEvent.ExternalId,
            Updated = changedEvent.ExternalUpdatedDate,
        };
    }

    private void HandleAccessAddressMunicipalCodeChanged(AccessAddressMunicipalCodeChanged changedEvent)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            MunicipalCode = changedEvent.MunicipalCode,
            Updated = changedEvent.ExternalUpdatedDate
        };
    }

    private void HandleAccessAddressStatusChanged(AccessAddressStatusChanged changedEvent)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            Status = changedEvent.Status.ToString(),
            Updated = changedEvent.ExternalUpdatedDate
        };
    }

    private void HandleAccessAddressRoadCodeChanged(AccessAddressRoadCodeChanged changedEvent)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            RoadCode = changedEvent.RoadCode,
            Updated = changedEvent.ExternalUpdatedDate
        };
    }

    private void HandleAccessAddressHouseNumberChanged(AccessAddressHouseNumberChanged changedEvent)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            HouseNumber = changedEvent.HouseNumber,
            Updated = changedEvent.ExternalUpdatedDate
        };
    }

    private void HandleAccessAddressCoordinateChanged(AccessAddressCoordinateChanged changedEvent)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            EastCoordinate = changedEvent.EastCoordinate,
            NorthCoordinate = changedEvent.NorthCoordinate,
            Updated = changedEvent.ExternalUpdatedDate
        };
    }

    private void HandleAccessAddressSupplementaryTownNameChanged(AccessAddressSupplementaryTownNameChanged changedEvent)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            TownName = changedEvent.SupplementaryTownName,
            Updated = changedEvent.ExternalUpdatedDate
        };
    }

    private void HandleAccessAddressPlotIdChanged(AccessAddressPlotIdChanged changedEvent)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            PlotId = changedEvent.PlotId,
            Updated = changedEvent.ExternalUpdatedDate
        };
    }

    private void HandleAccessAddressRoadIdChanged(AccessAddressRoadIdChanged changedEvent)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            RoadId = changedEvent.RoadId,
            Updated = changedEvent.ExternalUpdatedDate
        };
    }

    private void HandleAccessAddressDeleted(AccessAddressDeleted accessAddressDeleted)
    {
        var oldAccessAddress = IdToAccessAddress[accessAddressDeleted.Id];
        IdToAccessAddress[accessAddressDeleted.Id] = oldAccessAddress with
        {
            Deleted = true,
            Updated = accessAddressDeleted.ExternalUpdatedDate
        };
    }

    private void HandleUnitAddressCreated(UnitAddressCreated unitAddressCreated)
    {
        IdToUnitAddress.Add(
            unitAddressCreated.Id,
            new UnitAddress(
                ExternalId: unitAddressCreated.ExternalId,
                AccessAddressId: unitAddressCreated.AccessAddressId,
                Status: unitAddressCreated.Status.ToString(),
                FloorName: unitAddressCreated.FloorName,
                SuitName: unitAddressCreated.SuiteName,
                Created: unitAddressCreated.ExternalCreatedDate ?? throw new ArgumentException("External created date should never be null."),
                Updated: unitAddressCreated.ExternalUpdatedDate,
                Deleted: false));
    }

    private void HandleUnitAddressExternalIdChanged(UnitAddressExternalIdChanged unitAddressExternalIdChanged)
    {
        var unitAddress = IdToUnitAddress[unitAddressExternalIdChanged.Id];
        IdToUnitAddress[unitAddressExternalIdChanged.Id] = unitAddress with
        {
            ExternalId = unitAddressExternalIdChanged.ExternalId,
            Updated = unitAddressExternalIdChanged.ExternalUpdatedDate
        };
    }

    private void HandleUnitAddressAccessAddressIdChanged(UnitAddressAccessAddressIdChanged unitAddressAccessAddressIdChanged)
    {
        var unitAddress = IdToUnitAddress[unitAddressAccessAddressIdChanged.Id];
        IdToUnitAddress[unitAddressAccessAddressIdChanged.Id] = unitAddress with
        {
            AccessAddressId = unitAddressAccessAddressIdChanged.AccessAddressId,
            Updated = unitAddressAccessAddressIdChanged.ExternalUpdatedDate
        };
    }

    private void HandleUnitAddressStatusChanged(UnitAddressStatusChanged unitAddressStatusChanged)
    {
        var unitAddress = IdToUnitAddress[unitAddressStatusChanged.Id];
        IdToUnitAddress[unitAddressStatusChanged.Id] = unitAddress with
        {
            Status = unitAddressStatusChanged.Status.ToString(),
            Updated = unitAddressStatusChanged.ExternalUpdatedDate
        };
    }

    private void HandleUnitAddressFloorNameChanged(UnitAddressFloorNameChanged unitAddressFloorNameChanged)
    {
        var unitAddress = IdToUnitAddress[unitAddressFloorNameChanged.Id];
        IdToUnitAddress[unitAddressFloorNameChanged.Id] = unitAddress with
        {
            FloorName = unitAddressFloorNameChanged.FloorName,
            Updated = unitAddressFloorNameChanged.ExternalUpdatedDate
        };
    }

    private void HandleUnitAddressSuiteNameChanged(UnitAddressSuiteNameChanged unitAddressSuiteNameChanged)
    {
        var unitAddress = IdToUnitAddress[unitAddressSuiteNameChanged.Id];
        IdToUnitAddress[unitAddressSuiteNameChanged.Id] = unitAddress with
        {
            SuitName = unitAddressSuiteNameChanged.SuiteName,
            Updated = unitAddressSuiteNameChanged.ExternalUpdatedDate
        };
    }

    private void HandleUnitAddressDeleted(UnitAddressDeleted unitAddressDeleted)
    {
        var unitAddress = IdToUnitAddress[unitAddressDeleted.Id];
        IdToUnitAddress[unitAddressDeleted.Id] = unitAddress with
        {
            Deleted = true,
            Updated = unitAddressDeleted.ExternalUpdatedDate
        };
    }

    private void HandlePostCodeCreated(PostCodeCreated postCodeCreated)
    {
        IdToPostCode.Add(
            postCodeCreated.Id,
            new(postCodeCreated.Number, postCodeCreated.Name, false));
    }

    private void HandlePostCodeNameChanged(PostCodeNameChanged postCodeNameChanged)
    {
        var postCode = IdToPostCode[postCodeNameChanged.Id];
        IdToPostCode[postCodeNameChanged.Id] = postCode with
        {
            Name = postCodeNameChanged.Name
        };
    }

    private void HandlePostCodeDeleted(PostCodeDeleted postCodeDeleted)
    {
        var postCode = IdToPostCode[postCodeDeleted.Id];
        IdToPostCode[postCodeDeleted.Id] = postCode with
        {
            Deleted = true
        };
    }

    private void HandleRoadCreated(RoadCreated roadCreated)
    {
        var road = new Road(roadCreated.ExternalId, roadCreated.Name, false);
        IdToRoad.Add(roadCreated.Id, road);
    }

    private void HandleRoadNameChanged(RoadNameChanged roadNameChanged)
    {
        var road = IdToRoad[roadNameChanged.Id];
        IdToRoad[roadNameChanged.Id] = road with
        {
            Name = roadNameChanged.Name
        };
    }

    private void HandleRoadExternalIdChanged(RoadExternalIdChanged roadExternalIdChanged)
    {
        var road = IdToRoad[roadExternalIdChanged.Id];
        IdToRoad[roadExternalIdChanged.Id] = road with
        {
            ExternalId = roadExternalIdChanged.ExternalId
        };
    }

    private void HandleRoadDeleted(RoadDeleted roadDeleted)
    {
        var road = IdToRoad[roadDeleted.Id];
        IdToRoad[roadDeleted.Id] = road with
        {
            Deleted = true
        };
    }
}
