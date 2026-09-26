#!lua
-- KN-3 stage B apply (decisions D7/D8): one atomic batch per call.
--
-- ARGV: prefix, topic, partition, owner_fence, next_offset, now_ms, op_count,
-- then the ops, each as a fixed-width group led by its code (see `WIDTH`).
-- Every u64 (offsets, fences, generations, open times) travels as a decimal
-- string and is compared as one - never converted to a Lua number.
--
-- Pass 1 checks the owner fence and every op's expectations (pointer and
-- current entry); any mismatch applies nothing and returns the missed op
-- indexes. Pass 2 applies every op and the checkpoint. The `#!lua` shebang
-- (no `no-writes` flag) makes Redis refuse the whole script under `OOM`
-- (noeviction) before it runs, so a batch is never half-applied.

local prefix, topic, partition = ARGV[1], ARGV[2], ARGV[3]
local owner_fence, next_offset, now_ms = ARGV[4], ARGV[5], ARGV[6]
local op_count = tonumber(ARGV[7])

-- Decimal strings without leading zeros: -1, 0 or 1.
local function cmp(a, b)
  if #a ~= #b then
    return (#a < #b) and -1 or 1
  end
  if a == b then
    return 0
  end
  return (a < b) and -1 or 1
end

local function key(...)
  return prefix .. table.concat({...}, ':')
end

-- Decimal string arithmetic for small counters is done with INCRBY/HINCRBY
-- (Redis integers are exact 64-bit), never in Lua numbers.

local WIDTH = {
  -- S: set staging  lpk exp_ready exp_staging exp_fence new_staging
  S = 6,
  -- P: publish      lpk exp_ready exp_staging exp_fence
  P = 5,
  -- U: unpublish    lpk exp_ready exp_staging exp_fence (state gone: NOT_READY)
  U = 5,
  -- X: unstage      lpk exp_ready exp_staging exp_fence (discard an unpublished staging)
  X = 5,
  -- L: latest       lpk gen exp_ready exp_staging exp_fence exp_offset value topic_id partition offset
  L = 11,
  -- D: latest del   lpk gen exp_ready exp_staging exp_fence
  D = 6,
  -- B: bar row      lpk gen exp_ready exp_staging exp_fence bucket open_ms exp_trailer row is_final rk_append
  B = 13,
  -- N: bar note     lpk gen exp_ready exp_staging exp_fence open_ms rk_append conflict
  N = 9,
  -- F: floor        lpk gen exp_ready exp_staging exp_fence floor_ms buckets_csv boundary_bucket
  F = 9,
  -- W: source watermark  topic_id canonical_partition offset (KN-4 D27; no
  --    pointer expectation; raises `s|<topic_id>|<partition>` of this
  --    partition's checkpoint - every fact of a product of this state
  --    partition on that canonical partition at or below it is applied)
  W = 4,
  -- K: product source    lpk topic_id canonical_partition (KN-4 D27; a BAR
  --    product's canonical coordinate, generation independent)
  K = 4,
}

local ops = {}
local cursor = 8
for index = 1, op_count do
  local code = ARGV[cursor]
  local width = WIDTH[code]
  if not width then
    return redis.error_reply('KN3_BAD_OP ' .. tostring(code))
  end
  local op = {}
  for field = 0, width - 1 do
    op[field + 1] = ARGV[cursor + field]
  end
  ops[index] = op
  cursor = cursor + width
end

-- The partition owner: a zombie (older fence) applies nothing.
local current_owner = redis.call('GET', key('own', topic, partition))
if current_owner ~= owner_fence then
  return {'ZOMBIE', current_owner or ''}
end

local function pointer(lpk)
  local fields = redis.call('HMGET', key('ptr', lpk), 'ready', 'staging', 'fence')
  return fields[1] or '', fields[2] or '', fields[3] or '0'
end

local function pointer_matches(op)
  if op[1] == 'W' or op[1] == 'K' then
    return true
  end
  local ready, staging, fence = pointer(op[2])
  local base = (op[1] == 'S' or op[1] == 'P' or op[1] == 'U' or op[1] == 'X') and 3 or 4
  return ready == op[base] and staging == op[base + 1] and fence == op[base + 2]
end

-- The generation an op writes must be the product's ready or staging one.
local function writable(op)
  if op[1] == 'S' or op[1] == 'P' or op[1] == 'U' or op[1] == 'X' or op[1] == 'W' or op[1] == 'K' then
    return true
  end
  local gen = op[3]
  return gen ~= '' and (gen == op[4] or gen == op[5])
end

local function entry_matches(op)
  local code = op[1]
  if code == 'L' then
    local offset = redis.call('HGET', key('l', op[3], op[2]), 'o')
    return (offset or '') == op[7]
  elseif code == 'B' then
    local row = redis.call('HGET', key('b', op[3], op[2], op[7]), op[8])
    local trailer = row and string.sub(row, 1, 48) or ''
    return trailer == op[9]
  end
  return true
end

-- Pass 1: expectations.
local missed = {}
for index, op in ipairs(ops) do
  if not (pointer_matches(op) and writable(op) and entry_matches(op)) then
    missed[#missed + 1] = tostring(index)
  end
end
if #missed > 0 then
  return {'MISS', table.concat(missed, ',')}
end

local function raise_meta(meta, field, value)
  local current = redis.call('HGET', meta, field)
  if not current or cmp(value, current) > 0 then
    redis.call('HSET', meta, field, value)
  end
end

local function lower_meta(meta, field, value)
  local current = redis.call('HGET', meta, field)
  if not current or cmp(value, current) < 0 then
    redis.call('HSET', meta, field, value)
  end
end

local function append_rk(gen, lpk, open_ms, fact)
  if fact == '' then
    return
  end
  local rk = key('rk', gen, lpk)
  local current = redis.call('HGET', rk, open_ms)
  redis.call('HSET', rk, open_ms, current and (current .. ',' .. fact) or fact)
end

-- Pass 2: apply.
local results = {}
for index, op in ipairs(ops) do
  local code, lpk = op[1], op[2]
  if code == 'S' then
    -- A staging generation replaced here (interrupted build or rebuild) is
    -- retired in the same script, so no crash can lose it (D20).
    local replaced = redis.call('HGET', key('ptr', lpk), 'staging')
    if replaced and replaced ~= op[6] then
      redis.call('SADD', key('retire'), replaced .. '|' .. lpk)
    end
    redis.call('HSET', key('ptr', lpk), 'staging', op[6])
    -- A product's first pointer starts at fence 0.
    redis.call('HSETNX', key('ptr', lpk), 'fence', '0')
    -- The partition's product registry (cold build publish/unpublish).
    redis.call('SADD', key('parts', topic, partition), lpk)
    results[index] = 'STAGED'
  elseif code == 'P' then
    local ptr = key('ptr', lpk)
    local staged = redis.call('HGET', ptr, 'staging')
    redis.call('HSET', ptr, 'ready', staged)
    redis.call('HDEL', ptr, 'staging')
    redis.call('HINCRBY', ptr, 'fence', 1)
    -- The superseded generation (if any) is retired and returned for reclaim.
    if op[3] ~= '' then
      redis.call('SADD', key('retire'), op[3] .. '|' .. lpk)
    end
    results[index] = op[3]
  elseif code == 'U' then
    -- The product has no state any more: its pointer goes (NOT_READY) and
    -- its generations are returned for reclaim ("ready,staging").
    redis.call('DEL', key('ptr', lpk))
    redis.call('SREM', key('parts', topic, partition), lpk)
    for _, gone in ipairs({op[3], op[4]}) do
      if gone ~= '' then
        redis.call('SADD', key('retire'), gone .. '|' .. lpk)
      end
    end
    results[index] = op[3] .. ',' .. op[4]
  elseif code == 'X' then
    -- Only a staging generation that was never published (the CAS above
    -- proved staging = op[4]) is discarded; it is retired in this script
    -- (D23), never the ready one.
    redis.call('HDEL', key('ptr', lpk), 'staging')
    if op[4] ~= '' then
      redis.call('SADD', key('retire'), op[4] .. '|' .. lpk)
    end
    results[index] = 'UNSTAGED'
  elseif code == 'L' then
    redis.call('HSET', key('l', op[3], lpk), 'v', op[8], 't', op[9], 'p', op[10], 'o', op[11])
    results[index] = 'APPLIED'
  elseif code == 'D' then
    redis.call('DEL', key('l', op[3], lpk))
    results[index] = 'DELETED'
  elseif code == 'B' then
    local gen, bucket, open_ms = op[3], op[7], op[8]
    local meta = key('bm', gen, lpk)
    local floor = redis.call('HGET', meta, 'floor')
    if floor and cmp(open_ms, floor) < 0 then
      results[index] = 'STALE_BELOW_FLOOR'
    else
      redis.call('HSET', key('b', gen, lpk, bucket), open_ms, op[10])
      redis.call('HSET', key('bd', gen, lpk, bucket), open_ms, op[13])
      if op[9] == '' then
        redis.call('HINCRBY', meta, 'rows', 1)
      end
      lower_meta(meta, 'first', open_ms)
      raise_meta(meta, 'last', open_ms)
      if op[11] == '1' then
        raise_meta(meta, 'last_final', open_ms)
      end
      append_rk(gen, lpk, open_ms, op[12])
      results[index] = 'APPLIED'
    end
  elseif code == 'N' then
    local gen, open_ms = op[3], op[7]
    append_rk(gen, lpk, open_ms, op[8])
    if op[9] ~= '' then
      redis.call('HINCRBY', key('bm', gen, lpk), 'conflicts', 1)
      local list = key('cx', gen, lpk)
      redis.call('LPUSH', list, op[9])
      redis.call('LTRIM', list, 0, 99)
    end
    results[index] = 'NOTED'
  elseif code == 'F' then
    local gen, floor_ms = op[3], op[7]
    local meta = key('bm', gen, lpk)
    local current = redis.call('HGET', meta, 'floor')
    if current and cmp(floor_ms, current) <= 0 then
      results[index] = 'FLOOR_NOT_RAISED'
    else
      local removed = 0
      if op[8] ~= '' then
        for bucket in string.gmatch(op[8], '[^,]+') do
          local bucket_key = key('b', gen, lpk, bucket)
          removed = removed + redis.call('HLEN', bucket_key)
          redis.call('UNLINK', bucket_key, key('bd', gen, lpk, bucket))
        end
      end
      if op[9] ~= '' then
        local boundary = key('b', gen, lpk, op[9])
        local fields = redis.call('HKEYS', boundary)
        for _, open_ms in ipairs(fields) do
          if cmp(open_ms, floor_ms) < 0 then
            redis.call('HDEL', boundary, open_ms)
            redis.call('HDEL', key('bd', gen, lpk, op[9]), open_ms)
            removed = removed + 1
          end
        end
      end
      -- Extra fact keys below the floor are gone with their opens.
      local rk = key('rk', gen, lpk)
      for _, open_ms in ipairs(redis.call('HKEYS', rk)) do
        if cmp(open_ms, floor_ms) < 0 then
          redis.call('HDEL', rk, open_ms)
        end
      end
      redis.call('HSET', meta, 'floor', floor_ms)
      if removed > 0 then
        redis.call('HINCRBY', meta, 'rows', -removed)
      end
      -- `first` is a lower bound of the retained opens (bucket iteration).
      local first = redis.call('HGET', meta, 'first')
      if first and cmp(first, floor_ms) < 0 then
        redis.call('HSET', meta, 'first', floor_ms)
      end
      results[index] = 'FLOOR ' .. tostring(removed)
    end
  elseif code == 'W' then
    raise_meta(key('ckpt', topic, partition), 's|' .. op[2] .. '|' .. op[3], op[4])
    results[index] = 'WATERMARK'
  elseif code == 'K' then
    redis.call('HSET', key('src', lpk), 't', op[3], 'p', op[4])
    results[index] = 'SOURCE'
  end
end

redis.call('HSET', key('ckpt', topic, partition), 'next', next_offset, 'fence', owner_fence, 'at_ms', now_ms)
return {'OK', results}
