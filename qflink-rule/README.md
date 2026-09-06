# QFlink-Rule: Flink Job driven by rule

## Quick start

- [RuleEngine](./src/main/java/cn/syntomic/qflink/rule/RuleEngine.java)

## Design

- source
    - log
    - broadcast rule

- dynamic etl

- dynamic agg

## Dynamic window

The aggregate operator windows the keyed stream by the window of the rule, and the windows are
**sliced like the window table valued functions of Flink SQL**
([DynamicSliceAssigner](./src/main/java/cn/syntomic/qflink/rule/functions/windowing/DynamicSliceAssigner.java)).

A slice is the smallest non overlapping interval a window can be split into: the size for TUMBLE,
the step for SLIDE and CUMULATE. An element is accumulated into **one** slice, whatever the number
of windows it belongs to, and a window result is computed by merging the slices it is composed of.

```
CUMULATE(size = 4 min, step = 1 min)

elements   e1 e2        e3
slices    |    S1    |    S2    |    S3    |    S4    |     <- an element is stored once
windows   |    W1    |
          |          W2         |
          |                    W3          |
          |                              W4           |
```

| | before(one window per element) | now(one slice per element) |
| --- | --- | --- |
| CUMULATE 1 day / 1 min | up to 1440 accumulations per element | 1 |
| SLIDE 1 hour / 1 min | 60 accumulations per element | 1 |
| TUMBLE | 1 | 1 |

- a CUMULATE window merges the slice which just ended into the first slice of the window, so a step
  does not have to read the whole window again
- a SLIDE window merges the `size / step` slices it is composed of when it fires
- a slice is dropped once the last window it belongs to has been fired and `agg.window.allow-lateness`
  has elapsed

### Rule constraints

- **the window size must be an integral multiple of the step**, the same restriction the window
  table valued functions have. A rule which cannot be sliced is reported to the error side output
  when it arrives, not on every element.

### Mini batch

`agg.mini-batch.enabled` buffers the elements in memory and aggregates them **before** they reach
the state, like `table.exec.mini-batch.*` does for the window table valued functions
([MiniBatchWindowBuffer](./src/main/java/cn/syntomic/qflink/rule/functions/windowing/MiniBatchWindowBuffer.java)).
A slice is then read and written once per batch instead of once per element, which is what makes the
difference on a state backend where every access is a serialization(RocksDB).

| Option | Default | Description |
| --- | --- | --- |
| `agg.mini-batch.enabled` | `false` | Whether to buffer the elements before writing them to the state |
| `agg.mini-batch.size` | `1000` | Maximum number of buffered elements |
| `agg.mini-batch.allow-latency` | `1 s` | Maximum time an element stays in the buffer |

The buffer is flushed when it is full, after the allowed latency, **before a watermark**(the windows
of the watermark must see the elements) and **before a checkpoint barrier**(the buffer is not a
state).

> With the mini batch a `SINGLE` trigger emits one alert per slice and per batch, with the last
> element of the batch as the alert context, instead of one alert per element. `BATCH` triggers are
> not affected, they only fire on the window timers.

### Two phase aggregation

`agg.local-enabled` aggregates the slices **before the shuffle**, like the two phase strategy of
`table.optimizer.agg-phase-strategy` does for the window table valued functions. Only the
accumulators cross the network, not the elements:

```
                        agg.local-enabled = false
log ─► Co-Dynamic-ETL ──────── every element ────────► keyBy ─► Co-Dynamic-Agg

                        agg.local-enabled = true
log ─► Co-Dynamic-ETL ─► Co-Dynamic-Local-Agg ─► keyBy ─► Co-Dynamic-Agg
                         (rule_id, key, slice_end,        merges the partial
                          accumulators, extras)           accumulators into the window
```

- the first phase([LocalWindowAggOperator](./src/main/java/cn/syntomic/qflink/rule/operators/LocalWindowAggOperator.java))
  holds no keyed state, it buffers and flushes exactly like the mini batch does
- the second phase merges the partial accumulators into the slice state, everything after that is
  unchanged: slices, window timers, cleanup
- it pays off when a few keys carry most of the traffic, and it also merges the copies a log which
  matches several rules produces

> Every aggregate must be mergeable, they all are. Keep in mind that the partial accumulator of a
> `COUNT_DISTINCT` is the set of its values: with a high cardinality key, sending the set may cost
> more than sending the elements.

### Trigger

| Trigger | When | Result |
| --- | --- | --- |
| `BATCH` | at the end of every window | the complete window, a late element within the lateness fires its window again |
| `SINGLE` | on every element | the window which ends with the slice of the element |

> `SINGLE` emits **one** alert per element. A CUMULATE window used to emit one identical alert per
> pending step, and a SLIDE window one alert per window the element belonged to, now only the window
> with the most history is evaluated.
