using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Soenneker.Dtos.IdValuePair;
using Soenneker.Extensions.ValueTask;
using Soenneker.Utils.Json;
using Soenneker.Zelos.Abstract;

namespace Soenneker.Zelos.Container;

///<inheritdoc cref="IZelosContainer"/>
public sealed class ZelosContainer : IZelosContainer
{
    private readonly string _containerName;
    private ConcurrentDictionary<string, string> _items;
    private readonly IZelosDatabase _database;
    private readonly ILogger _logger;
    private bool _disposed;

    public ZelosContainer(string containerName, IZelosDatabase database, ILogger logger, List<IdValuePair>? existingData = null)
    {
        _containerName = containerName;
        _database = database;
        _logger = logger;

        if (existingData is null || existingData.Count == 0)
        {
            _items = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            _logger.LogWarning("No existing data found for container ({containerName})", containerName);
        }
        else
        {
            _items = new ConcurrentDictionary<string, string>(-1, existingData.Count, StringComparer.OrdinalIgnoreCase);

            foreach (IdValuePair data in existingData)
            {
                if (!_items.TryAdd(data.Id, data.Value))
                {
                    _logger.LogWarning("Duplicate key detected: {key} in container ({containerName})", data.Id, containerName);
                }
            }

            _logger.LogDebug("Loaded {count} items for container ({containerName})", _items.Count, containerName);
        }
    }

    public async ValueTask<string> AddItem(string id, string item, CancellationToken cancellationToken = default)
    {
        bool successful = _items.TryAdd(id, item);

        if (!successful)
            throw new Exception($"Failed to add item ({id})");

        await _database.MarkDirty(_containerName, cancellationToken).NoSync();
        return item;
    }

    public IQueryable<T> BuildQueryable<T>()
    {
        var result = new List<T>(_items.Count);

        foreach (KeyValuePair<string, string> item in _items)
        {
            var deserialized = JsonUtil.Deserialize<T>(item.Value);

            if (deserialized != null)
                result.Add(deserialized);
        }

        return result.AsQueryable();
    }

    public string? GetItem(string id)
    {
        return _items.GetValueOrDefault(id);
    }

    public string GetItemStrict(string id)
    {
        bool successful = _items.TryGetValue(id, out string? item);

        if (successful)
            return item!;

        throw new Exception($"Could not find item ({id})");
    }

    public async ValueTask<string?> UpdateItem(string id, string item, CancellationToken cancellationToken = default)
    {
        string? retrieved = GetItem(id);

        if (retrieved == null)
            return null;

        bool successful = _items.TryUpdate(id, item, retrieved);

        if (!successful)
            return null;

        await _database.MarkDirty(_containerName, cancellationToken).NoSync();

        return item;
    }

    public async ValueTask<string> UpdateItemStrict(string id, string item, CancellationToken cancellationToken = default)
    {
        string retrieved = GetItemStrict(id);

        bool successful = _items.TryUpdate(id, item, retrieved);

        if (!successful)
            throw new Exception($"Failed to update item ({id})");

        await _database.MarkDirty(_containerName, cancellationToken).NoSync();

        return item;
    }

    public async ValueTask DeleteItem(string id, CancellationToken cancellationToken = default)
    {
        bool successful = _items.TryRemove(id, out _);

        if (!successful)
            throw new Exception($"Failed to delete item ({id})");

        await _database.MarkDirty(_containerName, cancellationToken).NoSync();
    }

    public List<string> GetAllItems()
    {
        return _items.Values.ToList();
    }

    public List<string> GetAllIds()
    {
        return _items.Keys.ToList();
    }

    public List<IdValuePair> GetZelosItems()
    {
        var items = new List<IdValuePair>(_items.Count);

        foreach (KeyValuePair<string, string> item in _items)
        {
            items.Add(new IdValuePair { Id = item.Key, Value = item.Value });
        }

        return items;
    }

    public async ValueTask DeleteAllItems(CancellationToken cancellationToken = default)
    {
        _items.Clear();

        await _database.MarkDirty(_containerName, cancellationToken).NoSync();
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _logger.LogDebug("Disposing container ({containerName})", _containerName);

        _disposed = true;

        _items.Clear();
        _items = null!;

        GC.SuppressFinalize(this);
    }
}