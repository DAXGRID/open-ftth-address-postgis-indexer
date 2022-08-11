using Microsoft.Extensions.Logging;
using OpenFTTH.Core.Address;
using OpenFTTH.Core.Address.Events;
using OpenFTTH.EventSourcing;

namespace OpenFTTH.AddressPostgisProjector;

internal sealed record PostCode(string Code, string Name);
internal sealed record Road(string OfficialId, string Name);

internal sealed record AccessAddress(
    Guid Id,
    string? OfficialId,
    string MunicipalCode,
    AccessAddressStatus Status,
    string RoadCode,
    string HouseNumber,
    double EastCoordinate,
    double NorthCoordinate,
    string? TownName,
    string? PlotId,
    Guid RoadId,
    Guid PostCodeId,
    bool Deleted);

internal sealed class AddressPostgisProjection : ProjectionBase
{
    private uint _count;
    private ILogger<AddressPostgisProjection> _logger;

    public readonly Dictionary<Guid, PostCode> IdToPostCode = new();
    public readonly Dictionary<Guid, Road> IdToRoad = new();
    public readonly Dictionary<Guid, AccessAddress> IdToAddress = new();

    public AddressPostgisProjection(ILogger<AddressPostgisProjection> logger)
    {
        _logger = logger;

        ProjectEventAsync<PostCodeCreated>(ProjectAsync);
        ProjectEventAsync<PostCodeUpdated>(ProjectAsync);
        ProjectEventAsync<PostCodeDeleted>(ProjectAsync);

        ProjectEventAsync<RoadCreated>(ProjectAsync);
        ProjectEventAsync<RoadUpdated>(ProjectAsync);
        ProjectEventAsync<RoadDeleted>(ProjectAsync);

        ProjectEventAsync<AccessAddressCreated>(ProjectAsync);
        ProjectEventAsync<AccessAddressUpdated>(ProjectAsync);
        ProjectEventAsync<AccessAddressDeleted>(ProjectAsync);
    }

    private Task ProjectAsync(IEventEnvelope eventEnvelope)
    {
        _count++;

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
            default:
                throw new ArgumentException(
                    $"Could not handle typeof '{eventEnvelope.Data.GetType().Name}'");
        }

        return Task.CompletedTask;
    }

    private void HandleAccessAddressCreated(AccessAddressCreated accessAddressCreated)
    {
        IdToAddress.Add(
            accessAddressCreated.Id,
            new(Id: accessAddressCreated.Id,
                OfficialId: accessAddressCreated.OfficialId,
                MunicipalCode: accessAddressCreated.MunicipalCode,
                Status: accessAddressCreated.Status,
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
        var oldAccessAddress = IdToAddress[accessAddressUpdated.Id];
        IdToAddress[accessAddressUpdated.Id] = oldAccessAddress with
        {
            OfficialId = accessAddressUpdated.OfficialId,
            MunicipalCode = accessAddressUpdated.MunicipalCode,
            Status = accessAddressUpdated.Status,
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
        var oldAccessAddress = IdToAddress[accessAddressDeleted.Id];
        IdToAddress[accessAddressDeleted.Id] = oldAccessAddress with
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

    public override Task DehydrationFinishAsync()
    {
        _logger.LogInformation("Finished dehydration a total of {Count} events.", _count);
        return Task.CompletedTask;
    }
}
