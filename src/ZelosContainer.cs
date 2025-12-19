using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Soenneker.Atomics.ValueBools;
using Soenneker.Dtos.IdValuePair;
using Soenneker.Extensions.ValueTask;
using Soenneker.Utils.Json;
using Soenneker.Zelos.Abstract;

namespace Soenneker.Zelos.Container;

/// <inheritdoc cref="IZelosContainer"/>
public sealed class ZelosContainer : IZelosContainer, IDisposable
{
    private readonly string _containerName;
    private readonly IZelosDatabase _database;
    private readonly ILogger _logger;

    private readonly ConcurrentDictionary<string, string> _items;

    private ValueAtomicBool _disposed = new(false);

    public ZelosContainer(string containerName, IZelosDatabase database, ILogger logger, List<IdValuePair>? existingData = null)
    {
        _containerName = containerName;
        _database = database;
        _logger = logger;

        if (existingData is null || existingData.Count == 0)
        {
            _items = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            _logger.LogWarning("No existing data found for container ({containerName})", containerName);
            return;
        }

        // concurrencyLevel: a reasonable default; capacity: existing count
        _items = new ConcurrentDictionary<string, string>(
            concurrencyLevel: Environment.ProcessorCount,
            capacity: existingData.Count,
            comparer: StringComparer.OrdinalIgnoreCase);

        foreach (IdValuePair data in existingData)
        {
            if (!_items.TryAdd(data.Id, data.Value))
                _logger.LogWarning("Duplicate key detected: {key} in container ({containerName})", data.Id, containerName);
        }

        _logger.LogDebug("Loaded {count} items for container ({containerName})", _items.Count, containerName);
    }

    private void ThrowIfDisposed()
    {
        if (_disposed.Value)
            throw new ObjectDisposedException(nameof(ZelosContainer), $"Container '{_containerName}' is disposed.");
    }

    public async ValueTask<string> AddItem(string id, string item, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (!_items.TryAdd(id, item))
            throw new InvalidOperationException($"Failed to add item ({id})");

        await _database.MarkDirty(_containerName, cancellationToken).NoSync();
        return item;
    }

    public IQueryable<T> BuildQueryable<T>()
    {
        ThrowIfDisposed();

        var result = new List<T>(_items.Count);

        foreach (KeyValuePair<string, string> kvp in _items)
        {
            T? deserialized;

            try
            {
                deserialized = JsonUtil.Deserialize<T>(kvp.Value);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to deserialize item ({id}) in container ({containerName})", kvp.Key, _containerName);
                continue;
            }

            if (deserialized != null)
                result.Add(deserialized);
        }

        return result.AsQueryable();
    }

    public string? GetItem(string id)
    {
        ThrowIfDisposed();
        return _items.GetValueOrDefault(id);
    }

    public string GetItemStrict(string id)
    {
        ThrowIfDisposed();

        if (_items.TryGetValue(id, out string? item))
            return item!;

        throw new KeyNotFoundException($"Could not find item ({id})");
    }

    public async ValueTask<string?> UpdateItem(string id, string item, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        // Retry loop: item may change between TryGetValue and TryUpdate
        while (true)
        {
            if (!_items.TryGetValue(id, out string? existing))
                return null;

            if (_items.TryUpdate(id, item, existing))
                break;
        }

        await _database.MarkDirty(_containerName, cancellationToken).NoSync();
        return item;
    }

    public async ValueTask<string> UpdateItemStrict(string id, string item, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        // Retry loop
        while (true)
        {
            if (!_items.TryGetValue(id, out string? existing))
                throw new KeyNotFoundException($"Could not find item ({id})");

            if (_items.TryUpdate(id, item, existing))
                break;
        }

        await _database.MarkDirty(_containerName, cancellationToken).NoSync();
        return item;
    }

    public async ValueTask DeleteItem(string id, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (!_items.TryRemove(id, out _))
            throw new KeyNotFoundException($"Failed to delete item ({id})");

        await _database.MarkDirty(_containerName, cancellationToken).NoSync();
    }

    public List<string> GetAllItems()
    {
        ThrowIfDisposed();
        return _items.Values.ToList();
    }

    public List<string> GetAllIds()
    {
        ThrowIfDisposed();
        return _items.Keys.ToList();
    }

    public List<IdValuePair> GetZelosItems()
    {
        ThrowIfDisposed();

        var items = new List<IdValuePair>(_items.Count);

        foreach (KeyValuePair<string, string> kvp in _items)
            items.Add(new IdValuePair { Id = kvp.Key, Value = kvp.Value });

        return items;
    }

    public async ValueTask DeleteAllItems(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        _items.Clear();
        await _database.MarkDirty(_containerName, cancellationToken).NoSync();
    }

    public void Dispose()
    {
        // first caller wins
        if (!_disposed.TrySetTrue())
            return;

        _logger.LogDebug("Disposing container ({containerName})", _containerName);

        _items.Clear();
    }
}
