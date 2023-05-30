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
                HandleAccessAddressCreated(
                    accessAddressCreated,
                    eventEnvelope.EventTimestamp);
                break;
            case (AccessAddressExternalIdChanged accessAddressExternalIdChanged):
                HandleAccessAddressExternalIdChanged(
                    accessAddressExternalIdChanged,
                    eventEnvelope.EventTimestamp);
                break;
            case (AccessAddressMunicipalCodeChanged accessAddressMunicipalCodeChanged):
                HandleAccessAddressMunicipalCodeChanged(
                    accessAddressMunicipalCodeChanged,
                    eventEnvelope.EventTimestamp);
                break;
            case (AccessAddressStatusChanged accessAddressStatusChanged):
                HandleAccessAddressStatusChanged(
                    accessAddressStatusChanged,
                    eventEnvelope.EventTimestamp);
                break;
            case (AccessAddressRoadCodeChanged accessAddressRoadCodeChanged):
                HandleAccessAddressRoadCodeChanged(
                    accessAddressRoadCodeChanged,
                    eventEnvelope.EventTimestamp);
                break;
            case (AccessAddressHouseNumberChanged accessAddressHouseNumberChanged):
                HandleAccessAddressHouseNumberChanged(
                    accessAddressHouseNumberChanged,
                    eventEnvelope.EventTimestamp);
                break;
            case (AccessAddressCoordinateChanged accessAddressCoordinateChanged):
                HandleAccessAddressCoordinateChanged(
                    accessAddressCoordinateChanged,
                    eventEnvelope.EventTimestamp);
                break;
            case (AccessAddressSupplementaryTownNameChanged accessAddressSupplementaryTownNameChanged):
                HandleAccessAddressSupplementaryTownNameChanged(
                    accessAddressSupplementaryTownNameChanged,
                    eventEnvelope.EventTimestamp);
                break;
            case (AccessAddressPlotIdChanged handleAccessAddressPlotIdChanged):
                HandleAccessAddressPlotIdChanged(
                    handleAccessAddressPlotIdChanged,
                    eventEnvelope.EventTimestamp);
                break;
            case (AccessAddressRoadIdChanged handleAccessAddressRoadIdChanged):
                HandleAccessAddressRoadIdChanged(
                    handleAccessAddressRoadIdChanged,
                    eventEnvelope.EventTimestamp);
                break;
            case (AccessAddressDeleted accessAddressDeleted):
                HandleAccessAddressDeleted(
                    accessAddressDeleted,
                    eventEnvelope.EventTimestamp);
                break;
            case (UnitAddressCreated unitAddressCreated):
                HandleUnitAddressCreated(
                    unitAddressCreated,
                    eventEnvelope.EventTimestamp);
                break;
            case (UnitAddressExternalIdChanged unitAddressExternalIdChanged):
                HandleUnitAddressExternalIdChanged(
                    unitAddressExternalIdChanged,
                    eventEnvelope.EventTimestamp);
                break;
            case (UnitAddressAccessAddressIdChanged unitAddressAccessAddressIdChanged):
                HandleUnitAddressAccessAddressIdChanged(
                    unitAddressAccessAddressIdChanged,
                    eventEnvelope.EventTimestamp);
                break;
            case (UnitAddressStatusChanged unitAddressStatusChanged):
                HandleUnitAddressStatusChanged(
                    unitAddressStatusChanged,
                    eventEnvelope.EventTimestamp);
                break;
            case (UnitAddressFloorNameChanged unitAddressFloorNameChanged):
                HandleUnitAddressFloorNameChanged(
                    unitAddressFloorNameChanged,
                    eventEnvelope.EventTimestamp);
                break;
            case (UnitAddressSuiteNameChanged unitAddressSuiteNameChanged):
                HandleUnitAddressSuiteNameChanged(
                    unitAddressSuiteNameChanged,
                    eventEnvelope.EventTimestamp);
                break;
            case (UnitAddressDeleted unitAddressDeleted):
                HandleUnitAddressDeleted(
                    unitAddressDeleted,
                    eventEnvelope.EventTimestamp);
                break;
            default:
                throw new ArgumentException(
                    $"Could not handle typeof '{eventEnvelope.Data.GetType().Name}'");
        }

        return Task.CompletedTask;
    }

    private void HandleAccessAddressCreated(
        AccessAddressCreated accessAddressCreated,
        DateTime eventTimeStamp)
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
                Created: eventTimeStamp,
                Updated: null,
                Deleted: false));
    }

    private void HandleAccessAddressExternalIdChanged(
        AccessAddressExternalIdChanged changedEvent,
        DateTime eventTimeStamp)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            ExternalId = changedEvent.ExternalId,
            Updated = eventTimeStamp
        };
    }

    private void HandleAccessAddressMunicipalCodeChanged(
        AccessAddressMunicipalCodeChanged changedEvent,
        DateTime eventTimeStamp)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            MunicipalCode = changedEvent.MunicipalCode,
            Updated = eventTimeStamp
        };
    }

    private void HandleAccessAddressStatusChanged(
        AccessAddressStatusChanged changedEvent,
        DateTime eventTimeStamp)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            Status = changedEvent.Status.ToString(),
            Updated = eventTimeStamp
        };
    }

    private void HandleAccessAddressRoadCodeChanged(
        AccessAddressRoadCodeChanged changedEvent,
        DateTime eventTimeStamp)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            RoadCode = changedEvent.RoadCode,
            Updated = changedEvent.ExternalUpdatedDate
        };
    }

    private void HandleAccessAddressHouseNumberChanged(
        AccessAddressHouseNumberChanged changedEvent,
        DateTime eventTimeStamp)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            HouseNumber = changedEvent.HouseNumber,
            Updated = eventTimeStamp
        };
    }

    private void HandleAccessAddressCoordinateChanged(
        AccessAddressCoordinateChanged changedEvent,
        DateTime eventTimeStamp)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            EastCoordinate = changedEvent.EastCoordinate,
            NorthCoordinate = changedEvent.NorthCoordinate,
            Updated = eventTimeStamp
        };
    }

    private void HandleAccessAddressSupplementaryTownNameChanged(
        AccessAddressSupplementaryTownNameChanged changedEvent,
        DateTime eventTimeStamp)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            TownName = changedEvent.SupplementaryTownName,
            Updated = eventTimeStamp
        };
    }

    private void HandleAccessAddressPlotIdChanged(
        AccessAddressPlotIdChanged changedEvent,
        DateTime eventTimeStamp)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            PlotId = changedEvent.PlotId,
            Updated = eventTimeStamp
        };
    }

    private void HandleAccessAddressRoadIdChanged(
        AccessAddressRoadIdChanged changedEvent,
        DateTime eventTimeStamp)
    {
        var oldAccessAddress = IdToAccessAddress[changedEvent.Id];
        IdToAccessAddress[changedEvent.Id] = oldAccessAddress with
        {
            RoadId = changedEvent.RoadId,
            Updated = eventTimeStamp
        };
    }

    private void HandleAccessAddressDeleted(
        AccessAddressDeleted accessAddressDeleted,
        DateTime eventTimeStamp)
    {
        var oldAccessAddress = IdToAccessAddress[accessAddressDeleted.Id];
        IdToAccessAddress[accessAddressDeleted.Id] = oldAccessAddress with
        {
            Deleted = true,
            Updated = eventTimeStamp
        };
    }

    private void HandleUnitAddressCreated(
        UnitAddressCreated unitAddressCreated,
        DateTime eventTimeStamp)
    {
        IdToUnitAddress.Add(
            unitAddressCreated.Id,
            new UnitAddress(
                ExternalId: unitAddressCreated.ExternalId,
                AccessAddressId: unitAddressCreated.AccessAddressId,
                Status: unitAddressCreated.Status.ToString(),
                FloorName: unitAddressCreated.FloorName,
                SuitName: unitAddressCreated.SuiteName,
                Created: eventTimeStamp,
                Updated: null,
                Deleted: false));
    }

    private void HandleUnitAddressExternalIdChanged(
        UnitAddressExternalIdChanged unitAddressExternalIdChanged,
        DateTime eventTimeStamp)
    {
        var unitAddress = IdToUnitAddress[unitAddressExternalIdChanged.Id];
        IdToUnitAddress[unitAddressExternalIdChanged.Id] = unitAddress with
        {
            ExternalId = unitAddressExternalIdChanged.ExternalId,
            Updated = eventTimeStamp
        };
    }

    private void HandleUnitAddressAccessAddressIdChanged(
        UnitAddressAccessAddressIdChanged unitAddressAccessAddressIdChanged,
        DateTime eventTimeStamp)
    {
        var unitAddress = IdToUnitAddress[unitAddressAccessAddressIdChanged.Id];
        IdToUnitAddress[unitAddressAccessAddressIdChanged.Id] = unitAddress with
        {
            AccessAddressId = unitAddressAccessAddressIdChanged.AccessAddressId,
            Updated = eventTimeStamp
        };
    }

    private void HandleUnitAddressStatusChanged(
        UnitAddressStatusChanged unitAddressStatusChanged,
        DateTime eventTimeStamp)
    {
        var unitAddress = IdToUnitAddress[unitAddressStatusChanged.Id];
        IdToUnitAddress[unitAddressStatusChanged.Id] = unitAddress with
        {
            Status = unitAddressStatusChanged.Status.ToString(),
            Updated = eventTimeStamp
        };
    }

    private void HandleUnitAddressFloorNameChanged(
        UnitAddressFloorNameChanged unitAddressFloorNameChanged,
        DateTime eventTimeStamp)
    {
        var unitAddress = IdToUnitAddress[unitAddressFloorNameChanged.Id];
        IdToUnitAddress[unitAddressFloorNameChanged.Id] = unitAddress with
        {
            FloorName = unitAddressFloorNameChanged.FloorName,
            Updated = eventTimeStamp
        };
    }

    private void HandleUnitAddressSuiteNameChanged(
        UnitAddressSuiteNameChanged unitAddressSuiteNameChanged,
        DateTime eventTimeStamp)
    {
        var unitAddress = IdToUnitAddress[unitAddressSuiteNameChanged.Id];
        IdToUnitAddress[unitAddressSuiteNameChanged.Id] = unitAddress with
        {
            SuitName = unitAddressSuiteNameChanged.SuiteName,
            Updated = eventTimeStamp
        };
    }

    private void HandleUnitAddressDeleted(
        UnitAddressDeleted unitAddressDeleted,
        DateTime eventTimeStamp)
    {
        var unitAddress = IdToUnitAddress[unitAddressDeleted.Id];
        IdToUnitAddress[unitAddressDeleted.Id] = unitAddress with
        {
            Deleted = true,
            Updated = eventTimeStamp
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
