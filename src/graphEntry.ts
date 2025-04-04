import { HomeAssistant } from 'custom-card-helpers';
import {
  ChartCardSeriesConfig,
  EntityCachePoints,
  EntityEntryCache,
  HassHistory,
  HassHistoryEntry,
  HistoryBuckets,
  HistoryPoint,
  Statistics,
  StatisticValue,
} from './types';
import { compress, decompress, log } from './utils';
import localForage from 'localforage';
import { HassEntity } from 'home-assistant-js-websocket';
import { DateRange } from 'moment-range';
import { DEFAULT_STATISTICS_PERIOD, DEFAULT_STATISTICS_TYPE, moment } from './const';
import parse from 'parse-duration';
import SparkMD5 from 'spark-md5';
import { ChartCardSpanExtConfig, StatisticsPeriod } from './types-config';
import * as pjson from '../package.json';

export default class GraphEntry {
  private _computedHistory?: EntityCachePoints;

  private _hass?: HomeAssistant;

  private _entityID: string;

  private _entityState?: HassEntity;

  private _updating = false;

  private _cache: boolean;

  // private _hoursToShow: number;

  private _graphSpan: number;

  private _useCompress = false;

  private _index: number;

  private _config: ChartCardSeriesConfig;

  private _func: (item: EntityCachePoints) => number;

  private _realStart: Date;

  private _realEnd: Date;

  private _groupByDurationMs: number;

  private _md5Config: string;

  constructor(
    index: number,
    graphSpan: number,
    cache: boolean,
    config: ChartCardSeriesConfig,
    span: ChartCardSpanExtConfig | undefined,
  ) {
    const aggregateFuncMap = {
      avg: this._average,
      max: this._maximum,
      min: this._minimum,
      first: this._first,
      last: this._last,
      sum: this._sum,
      median: this._median,
      delta: this._delta,
      diff: this._diff,
    };
    this._index = index;
    this._cache = config.statistics ? false : cache;
    this._entityID = config.entity;
    this._graphSpan = graphSpan;
    this._config = config;
    this._func = aggregateFuncMap[config.group_by.func];
    this._realEnd = new Date();
    this._realStart = new Date();
    // Valid because tested during init;
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    this._groupByDurationMs = parse(this._config.group_by.duration)!;
    this._md5Config = SparkMD5.hash(`${this._graphSpan}${JSON.stringify(this._config)}${JSON.stringify(span)}`);
  }

  set hass(hass: HomeAssistant) {
    this._hass = hass;
    this._entityState = this._hass.states[this._entityID];
  }

  get history(): EntityCachePoints {
    return this._computedHistory || [];
  }

  get index(): number {
    return this._index;
  }

  get start(): Date {
    return this._realStart;
  }

  get end(): Date {
    return this._realEnd;
  }

  set cache(cache: boolean) {
    this._cache = this._config.statistics ? false : cache;
  }

  get lastState(): number | null {
    return this.history.length > 0 ? this.history[this.history.length - 1][1] : null;
  }

  public nowValue(now: number, before: boolean): number | null {
    if (this.history.length === 0) return null;
    const index = this.history.findIndex((point, index, arr) => {
      if (!before && point[0] > now) return true;
      if (before && point[0] < now && arr[index + 1] && arr[index + 1][0] > now) return true;
      return false;
    });
    if (index === -1) return null;
    return this.history[index][1];
  }

  get min(): number | undefined {
    if (!this._computedHistory || this._computedHistory.length === 0) return undefined;
    return Math.min(...this._computedHistory.flatMap((item) => (item[1] === null ? [] : [item[1]])));
  }

  get max(): number | undefined {
    if (!this._computedHistory || this._computedHistory.length === 0) return undefined;
    return Math.max(...this._computedHistory.flatMap((item) => (item[1] === null ? [] : [item[1]])));
  }

  public minMaxWithTimestamp(
    start: number,
    end: number,
    offset: number,
  ): { min: HistoryPoint; max: HistoryPoint } | undefined {
    if (!this._computedHistory || this._computedHistory.length === 0) return undefined;
    if (this._computedHistory.length === 1)
      return { min: [start, this._computedHistory[0][1]], max: [end, this._computedHistory[0][1]] };
    const minMax = this._computedHistory.reduce(
      (acc: { min: HistoryPoint; max: HistoryPoint }, point) => {
        if (point[1] === null) return acc;
        if (point[0] > end || point[0] < start) return acc;
        if (acc.max[1] === null || acc.max[1] < point[1]) acc.max = [...point];
        if (acc.min[1] === null || (point[1] !== null && acc.min[1] > point[1])) acc.min = [...point];
        return acc;
      },
      { min: [0, null], max: [0, null] },
    );
    if (offset) {
      if (minMax.min[0]) minMax.min[0] -= offset;
      if (minMax.max[0]) minMax.max[0] -= offset;
    }
    return minMax;
  }

  public minMaxWithTimestampForYAxis(start: number, end: number): { min: HistoryPoint; max: HistoryPoint } | undefined {
    if (!this._computedHistory || this._computedHistory.length === 0) return undefined;
    let lastTimestampBeforeStart = start;
    const lastHistoryIndexBeforeStart =
      this._computedHistory.findIndex((hist) => {
        return hist[0] >= start;
      }) - 1;
    if (lastHistoryIndexBeforeStart >= 0)
      lastTimestampBeforeStart = this._computedHistory[lastHistoryIndexBeforeStart][0];
    return this.minMaxWithTimestamp(lastTimestampBeforeStart, end, 0);
  }

  private async _getCache(key: string, compressed: boolean): Promise<EntityEntryCache | undefined> {
    const data: EntityEntryCache | undefined | null = await localForage.getItem(
      `${key}_${this._md5Config}${compressed ? '' : '-raw'}`,
    );
    return data ? (compressed ? decompress(data) : data) : undefined;
  }

  private async _setCache(
    key: string,
    data: EntityEntryCache,
    compressed: boolean,
  ): Promise<string | EntityEntryCache> {
    return compressed
      ? localForage.setItem(`${key}_${this._md5Config}`, compress(data))
      : localForage.setItem(`${key}_${this._md5Config}-raw`, data);
  }

  public async _updateHistory(start: Date, end: Date): Promise<boolean> {
    let startHistory = new Date(start);
    if (this._config.group_by.func !== 'raw') {
      const range = end.getTime() - start.getTime();
      const nbBuckets = Math.floor(range / this._groupByDurationMs) + (range % this._groupByDurationMs > 0 ? 1 : 0);
      startHistory = new Date(end.getTime() - (nbBuckets + 1) * this._groupByDurationMs);
    }
    if (!this._entityState || this._updating) return false;
    this._updating = true;

    if (this._config.ignore_history) {
      let currentState: null | number | string = null;
      if (this._config.attribute) {
        currentState = this._entityState.attributes?.[this._config.attribute];
      } else {
        currentState = this._entityState.state;
      }
      if (this._config.transform) {
        currentState = this._applyTransform(currentState, this._entityState);
      }
      let stateParsed: number | null = parseFloat(currentState as string);
      stateParsed = !Number.isNaN(stateParsed) ? stateParsed : null;
      this._computedHistory = [[new Date(this._entityState.last_updated).getTime(), stateParsed]];
      this._updating = false;
      return true;
    }

    let history: EntityEntryCache | undefined = undefined;

    if (this._config.data_generator) {
      history = await this._generateData(start, end);
    } else {
      this._realStart = new Date(start);
      this._realEnd = new Date(end);

      let skipInitialState = false;

      history = this._cache ? await this._getCache(this._entityID, this._useCompress) : undefined;

      if (history && history.span === this._graphSpan) {
        const currDataIndex = history.data.findIndex(
          (item) => item && new Date(item[0]).getTime() > startHistory.getTime(),
        );
        if (currDataIndex !== -1) {
          // skip initial state when fetching recent/not-cached data
          skipInitialState = true;
        }
        if (currDataIndex > 4) {
          // >4 so that the graph has some more history
          history.data = history.data.slice(currDataIndex === 0 ? 0 : currDataIndex - 4);
        } else if (currDataIndex === -1) {
          // there was no state which could be used in current graph so clearing
          history.data = [];
        }
      } else {
        history = undefined;
      }
      const usableCache = !!(
        history &&
        history.data &&
        history.data.length !== 0 &&
        history.data[history.data.length - 1]
      );

      // if data in cache, get data from last data's time + 1ms
      const fetchStart = usableCache
        ? // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
          new Date(history!.data[history!.data.length - 1][0] + 1)
        : new Date(startHistory.getTime() + (this._config.group_by.func !== 'raw' ? 0 : -1));
      const fetchEnd = end;

      let newStateHistory: EntityCachePoints = [];
      let updateGraphHistory = false;

      if (this._config.statistics) {
        const newHistory = await this._fetchStatistics(fetchStart, fetchEnd, this._config.statistics.period);
        if (newHistory && newHistory.length > 0) {
          updateGraphHistory = true;
          let lastNonNull: number | null = null;
          if (history && history.data && history.data.length > 0) {
            lastNonNull = history.data[history.data.length - 1][1];
          }
          newStateHistory = newHistory.map((item) => {
            let stateParsed: number | null = null;
            [lastNonNull, stateParsed] = this._transformAndFill(
              item[this._config.statistics?.type || DEFAULT_STATISTICS_TYPE],
              item,
              lastNonNull,
            );

            let displayDate: Date | null = null;
            const startDate = new Date(item.start);
            
            // Special handling for stacked columns with week/month periods
            const isStackedColumn = 
              this._config.type === 'column' && 
              this._config.stack_group !== undefined && 
              this._config.stack_group !== '';
              
            if (isStackedColumn && (this._config.statistics?.period === 'week' || this._config.statistics?.period === 'month')) {
              // For stacked columns with week/month periods, we need to use consistent timestamps
              // to ensure proper z-index rendering across all series
              const normalizedDate = new Date(startDate);
              normalizedDate.setUTCHours(12, 0, 0, 0); // Use consistent noon time for all series
              displayDate = normalizedDate;
            } else if (!this._config.statistics?.align || this._config.statistics?.align === 'middle') {
              // Standard middle alignment for non-stacked columns
              if (this._config.statistics?.period === '5minute') {
                displayDate = new Date(startDate.getTime() + 150000); // 2min30s
              } else if (!this._config.statistics?.period || this._config.statistics.period === 'hour') {
                displayDate = new Date(startDate.getTime() + 1800000); // 30min
              } else if (this._config.statistics.period === 'day') {
                displayDate = new Date(startDate.getTime() + 43200000); // 12h
              } else if (this._config.statistics.period === 'week') {
                displayDate = new Date(startDate.getTime() + 259200000); // 3.5d
              } else {
                displayDate = new Date(startDate.getTime() + 1296000000); // 15d
              }
            } else if (this._config.statistics.align === 'start') {
              displayDate = new Date(item.start);
            } else {
              displayDate = new Date(item.end);
            }

            // For stacked column charts, ensure null values become 0 to ensure proper stacking
            if (isStackedColumn && stateParsed === null) {
              stateParsed = 0;
            }

            return [displayDate.getTime(), !Number.isNaN(stateParsed) ? stateParsed : null];
          });
        }
      } else {
        const newHistory = await this._fetchRecent(
          fetchStart,
          fetchEnd,
          this._config.attribute || this._config.transform ? false : skipInitialState,
        );
        if (newHistory && newHistory[0] && newHistory[0].length > 0) {
          updateGraphHistory = true;
          /*
          hack because HA doesn't return anything if skipInitialState is false
          when retrieving for attributes so we retrieve it and we remove it.
          */
          if ((this._config.attribute || this._config.transform) && skipInitialState) {
            newHistory[0].shift();
          }
          let lastNonNull: number | null = null;
          if (history && history.data && history.data.length > 0) {
            lastNonNull = history.data[history.data.length - 1][1];
          }
          newStateHistory = newHistory[0].map((item) => {
            let currentState: unknown = null;
            if (this._config.attribute) {
              if (item.attributes && item.attributes[this._config.attribute] !== undefined) {
                currentState = item.attributes[this._config.attribute];
              }
            } else {
              currentState = item.state;
            }
            let stateParsed: number | null = null;
            [lastNonNull, stateParsed] = this._transformAndFill(currentState, item, lastNonNull);

            if (this._config.attribute) {
              return [new Date(item.last_updated).getTime(), !Number.isNaN(stateParsed) ? stateParsed : null];
            } else {
              return [new Date(item.last_changed).getTime(), !Number.isNaN(stateParsed) ? stateParsed : null];
            }
          });
        }
      }

      if (updateGraphHistory) {
        if (history?.data.length) {
          history.span = this._graphSpan;
          history.last_fetched = new Date();
          history.card_version = pjson.version;
          if (history.data.length !== 0) {
            history.data.push(...newStateHistory);
          }
        } else {
          history = {
            span: this._graphSpan,
            card_version: pjson.version,
            last_fetched: new Date(),
            data: newStateHistory,
          };
        }

        if (this._cache) {
          await this._setCache(this._entityID, history, this._useCompress).catch((err) => {
            log(err);
            localForage.clear();
          });
        }
      }
    }

    if (!history || history.data.length === 0) {
      this._updating = false;
      this._computedHistory = undefined;
      return false;
    }
    if (this._config.group_by.func !== 'raw') {
      const res: EntityCachePoints = this._dataBucketer(history, moment.range(startHistory, end)).map((bucket) => {
        return [bucket.timestamp, this._func(bucket.data)];
      });
      
      // For stacked columns in statistics mode, ensure nulls are converted to zeros
      // This is crucial for proper stacking when using statistics
      const isStackedColumn = 
        this._config.type === 'column' && 
        this._config.stack_group !== undefined && 
        this._config.stack_group !== '';
      
      if (isStackedColumn && this._config.statistics) {
        // Replace null values with zeros for proper stacking
        for (let i = 0; i < res.length; i++) {
          if (res[i][1] === null) {
            res[i][1] = 0;
          }
        }
      } else if ([undefined, 'line', 'area'].includes(this._config.type)) {
        // For non-stacked column charts, we can remove leading nulls
        while (res.length > 0 && res[0][1] === null) res.shift();
      }
      
      this._computedHistory = res;
    } else {
      this._computedHistory = history.data;
    }
    this._updating = false;
    return true;
  }

  private _transformAndFill(
    currentState: unknown,
    item: HassHistoryEntry | StatisticValue,
    lastNonNull: number | null,
  ): [number | null, number | null] {
    if (this._config.transform) {
      currentState = this._applyTransform(currentState, item);
    }
    let stateParsed: number | null = parseFloat(currentState as string);
    stateParsed = !Number.isNaN(stateParsed) ? stateParsed : null;
    
    const isStackedColumn = 
      this._config.type === 'column' && 
      this._config.stack_group !== undefined && 
      this._config.stack_group !== '';
    
    if (stateParsed === null) {
      if (this._config.fill_raw === 'zero' || (isStackedColumn && this._config.statistics)) {
        // For stacked column charts, especially with statistics, use 0 for null values
        stateParsed = 0;
      } else if (this._config.fill_raw === 'last') {
        stateParsed = lastNonNull;
      }
    } else {
      lastNonNull = stateParsed;
    }
    return [lastNonNull, stateParsed];
  }

  private _applyTransform(value: unknown, historyItem: HassHistoryEntry | StatisticValue): number | null {
    return new Function('x', 'hass', 'entity', `'use strict'; ${this._config.transform}`).call(
      this,
      value,
      this._hass,
      historyItem,
    );
  }

  private async _fetchRecent(
    start: Date | undefined,
    end: Date | undefined,
    skipInitialState: boolean,
  ): Promise<HassHistory | undefined> {
    let url = 'history/period';
    if (start) url += `/${start.toISOString()}`;
    url += `?filter_entity_id=${this._entityID}`;
    if (end) url += `&end_time=${end.toISOString()}`;
    if (skipInitialState) url += '&skip_initial_state';
    url += '&significant_changes_only=0';
    return this._hass?.callApi('GET', url);
  }

  private async _generateData(start: Date, end: Date): Promise<EntityEntryCache> {
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
    let data;
    try {
      const datafn = new AsyncFunction(
        'entity',
        'start',
        'end',
        'hass',
        'moment',
        `'use strict'; ${this._config.data_generator}`,
      );
      data = await datafn(this._entityState, start, end, this._hass, moment);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (e: any) {
      const funcTrimmed =
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        this._config.data_generator!.length <= 100
          ? // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
            this._config.data_generator!.trim()
          : // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
            `${this._config.data_generator!.trim().substring(0, 98)}...`;
      e.message = `${e.name}: ${e.message} in '${funcTrimmed}'`;
      e.name = 'Error';
      throw e;
    }
    return {
      span: 0,
      card_version: pjson.version,
      last_fetched: new Date(),
      data,
    };
  }

  private async _fetchStatistics(
    start: Date | undefined,
    end: Date | undefined,
    period: StatisticsPeriod = DEFAULT_STATISTICS_PERIOD,
  ): Promise<StatisticValue[] | undefined> {
    const statistics = await this._hass?.callWS<Statistics>({
      type: 'recorder/statistics_during_period',
      start_time: start?.toISOString(),
      end_time: end?.toISOString(),
      statistic_ids: [this._entityID],
      period,
    });
    if (statistics && this._entityID in statistics) {
      const result = statistics[this._entityID];
      
      // Special handling for week/month periods with stacked columns
      // This is critical to ensure proper stacking order across all series
      const isStackedColumn = 
        this._config.type === 'column' && 
        this._config.stack_group !== undefined && 
        this._config.stack_group !== '';
        
      if (isStackedColumn && result && (period === 'week' || period === 'month')) {
        // Make sure we have consistent timeline points by ensuring start/end timestamps
        // are predictable and consistent across all series
        if (result.length > 0) {
          // Normalize all timestamps to ensure consistent stacking
          // This is essential for longer periods (week/month) where inconsistent
          // timestamps can lead to z-index rendering issues
          for (const item of result) {
            // Convert all timestamps to exact UTC day boundaries
            // This ensures consistency across all series
            const day = new Date(item.start);
            day.setUTCHours(0, 0, 0, 0);
            item.start = day.toISOString();
            
            // Also ensure consistent end times for proper stacking
            if (item.end) {
              const endDay = new Date(item.end);
              endDay.setUTCHours(23, 59, 59, 999);
              item.end = endDay.toISOString();
            }
          }
        }
      }
      
      return result;
    }
    return undefined;
  }

  private _dataBucketer(history: EntityEntryCache, timeRange: DateRange): HistoryBuckets {
    const ranges = Array.from(timeRange.reverseBy('milliseconds', { step: this._groupByDurationMs })).reverse();
    // const res: EntityCachePoints[] = [[]];
    let buckets: HistoryBuckets = [];
    ranges.forEach((range, index) => {
      buckets[index] = { timestamp: range.valueOf(), data: [] };
    });
    history?.data.forEach((entry) => {
      buckets.some((bucket, index) => {
        if (bucket.timestamp > entry[0] && index > 0) {
          if (entry[0] >= buckets[index - 1].timestamp) {
            buckets[index - 1].data.push(entry);
            return true;
          }
        }
        return false;
      });
    });
    let lastNonNullBucketValue: number | null = null;
    const now = new Date().getTime();
    
    // Check if this is a stacked column chart that needs zero filling for proper stacking
    const isStackedColumn = 
      this._config.type === 'column' && 
      this._config.stack_group !== undefined && 
      this._config.stack_group !== '';
    
    buckets.forEach((bucket, index) => {
      if (bucket.data.length === 0) {
        if (this._config.group_by.fill === 'last' && (bucket.timestamp <= now || this._config.data_generator)) {
          bucket.data[0] = [bucket.timestamp, lastNonNullBucketValue];
        } else if (this._config.group_by.fill === 'zero' && (bucket.timestamp <= now || this._config.data_generator)) {
          bucket.data[0] = [bucket.timestamp, 0];
        } else if (this._config.group_by.fill === 'null') {
          // For column charts with stacking, ensure we always have a data point with value 0
          // This ensures column charts stack properly even with missing data
          if (isStackedColumn) {
            bucket.data[0] = [bucket.timestamp, 0];
          } else {
            bucket.data[0] = [bucket.timestamp, null];
          }
        }
      } else {
        lastNonNullBucketValue = bucket.data.slice(-1)[0][1];
      }
      if (this._config.group_by.start_with_last) {
        if (index > 0) {
          if (bucket.data.length === 0 || bucket.data[0][0] !== bucket.timestamp) {
            const prevBucketData = buckets[index - 1].data;
            bucket.data.unshift([bucket.timestamp, prevBucketData[prevBucketData.length - 1][1]]);
          }
        } else {
          const firstIndexAfter = history.data.findIndex((entry) => {
            if (entry[0] > bucket.timestamp) return true;
            return false;
          });
          if (firstIndexAfter > 0) {
            bucket.data.unshift([bucket.timestamp, history.data[firstIndexAfter - 1][1]]);
          }
        }
      }
    });
    buckets.shift();
    buckets.pop();
    
    // Handle series with statistics specially for stacked columns
    if (isStackedColumn && this._config.statistics) {
      // For stacked column charts with statistics, we need special handling to ensure proper stacking
      
      // Fill in all empty buckets with zeros
      for (let i = 0; i < buckets.length; i++) {
        if (buckets[i].data.length === 0 && buckets[i].timestamp <= now) {
          buckets[i].data = [[buckets[i].timestamp, 0]];
        }
      }
      
      // Special handling for statistics mode with stacked columns:
      // We need to ensure all timestamps are normalized to have the exact same timestamps
      // otherwise they won't stack properly
      
      // If this series has a missing data point but other series might have it,
      // we need to explicitly add data points with value 0 for all possible timestamps
      // This is crucial for proper stacking of columns
      if (buckets.length > 0) {
        // Special handling for week/month statistics which need more aggressive timestamp normalization
        if (this._config.statistics?.period === 'week' || this._config.statistics?.period === 'month') {
          // For week/month periods, we need precise timestamp normalization
          // to prevent columns from showing in front of each other
          
          // 1. Collect all non-normalized timestamps
          const rawTimestamps = buckets
            .filter(bucket => bucket.data.length > 0)
            .map(bucket => bucket.data[0][0]);
            
          // 2. Create normalized timestamps that are exactly the same across series
          // This is critical to ensure proper stacking order
          const normalizedTimestamps: number[] = [];
          rawTimestamps.forEach(timestamp => {
            // Standardize to noon UTC
            const normalizedDate = new Date(timestamp);
            normalizedDate.setUTCHours(12, 0, 0, 0);
            normalizedTimestamps.push(normalizedDate.getTime());
          });
          
          // 3. Create new buckets with normalized timestamps
          const normalizedBuckets: HistoryBuckets = [];
          for (const timestamp of Array.from(new Set(normalizedTimestamps)).sort()) {
            // Find the closest matching bucket
            const matchingBucket = buckets.find(bucket => {
              if (bucket.data.length === 0) return false;
              
              // Match within 24 hours to handle slight time differences
              return Math.abs(bucket.data[0][0] - timestamp) < 86400000;
            });
            
            if (matchingBucket) {
              // Create a new bucket with the normalized timestamp but original value
              const newValue = matchingBucket.data[0][1];
              normalizedBuckets.push({
                timestamp: matchingBucket.timestamp,
                data: [[timestamp, newValue]]
              });
            } else {
              // Create a zero entry for this timestamp
              normalizedBuckets.push({
                timestamp: timestamp,
                data: [[timestamp, 0]]
              });
            }
          }
          
          // Replace with normalized buckets to ensure proper stacking
          if (normalizedBuckets.length > 0) {
            buckets = normalizedBuckets;
          }
        } else {
          // For other periods, use the original approach
          // Collect a sorted list of all timestamps that should be considered
          const allBucketTimestamps: number[] = buckets
            .filter(bucket => bucket.data.length > 0)
            .map(bucket => bucket.data[0][0])
            .sort((a, b) => a - b);
          
          // Ensure we have exactly these timestamps in our data
          // This is critical for statistics mode where different series might have different timestamp sets
          if (allBucketTimestamps.length > 0) {
            // We need to make a copy because we'll be modifying the buckets array
            const modifiedBuckets: HistoryBuckets = [];
            
            // Add a data point (with value 0) for each timestamp
            for (const timestamp of allBucketTimestamps) {
              const existingBucket = buckets.find(b => b.data.length > 0 && b.data[0][0] === timestamp);
              if (existingBucket) {
                modifiedBuckets.push(existingBucket);
              } else {
                // If we don't already have this timestamp, add a zero-value data point
                modifiedBuckets.push({
                  timestamp: timestamp,
                  data: [[timestamp, 0]]
                });
              }
            }
            
            // Replace the original buckets with our normalized version
            buckets = modifiedBuckets;
          }
        }
      }
      
      // The critical fix: Ensure all values are filled with zeros when there should be data
      // This ensures the columns will stack properly and have the correct z-index
      if (this._config.statistics) {
        // Get data range for all series in statistics mode
        const startBucket = buckets.reduce((min, bucket) => {
          if (bucket.data.length > 0 && (min === null || bucket.data[0][0] < min)) {
            return bucket.data[0][0];
          }
          return min;
        }, null as number | null);
        
        const endBucket = buckets.reduce((max, bucket) => {
          if (bucket.data.length > 0 && (max === null || bucket.data[0][0] > max)) {
            return bucket.data[0][0];
          }
          return max;
        }, null as number | null);
        
        // Only proceed if we have valid start and end times
        if (startBucket !== null && endBucket !== null) {
          // Make sure every timestamp in the range has a data point
          // This is crucial for proper z-index ordering in stacked columns
          for (let i = 0; i < buckets.length; i++) {
            const bucket = buckets[i];
            if (bucket.timestamp > startBucket && bucket.timestamp <= endBucket && bucket.data.length === 0) {
              bucket.data = [[bucket.timestamp, 0]];
            }
          }
          
          // Ensure buckets with data are maintained in chronological order
          buckets.sort((a, b) => {
            if (a.data.length === 0 && b.data.length === 0) return 0;
            if (a.data.length === 0) return 1;
            if (b.data.length === 0) return -1;
            return a.data[0][0] - b.data[0][0];
          });
        }
      }
    }
    
    // Remove nulls at the end
    while (
      buckets.length > 0 &&
      (buckets[buckets.length - 1].data.length === 0 ||
        (buckets[buckets.length - 1].data.length === 1 && buckets[buckets.length - 1].data[0][1] === null))
    ) {
      buckets.pop();
    }
    return buckets;
  }

  private _sum(items: EntityCachePoints): number {
    if (items.length === 0) return 0;
    let lastIndex = 0;
    return items.reduce((sum, entry, index) => {
      let val = 0;
      if (entry && entry[1] === null) {
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        val = items[lastIndex][1]!;
      } else {
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        val = entry[1]!;
        lastIndex = index;
      }
      return sum + val;
    }, 0);
  }

  private _average(items: EntityCachePoints): number | null {
    const nonNull = this._filterNulls(items);
    if (nonNull.length === 0) return null;
    return this._sum(nonNull) / nonNull.length;
  }

  private _minimum(items: EntityCachePoints): number | null {
    let min: number | null = null;
    items.forEach((item) => {
      if (item[1] !== null)
        if (min === null) min = item[1];
        else min = Math.min(item[1], min);
    });
    return min;
  }

  private _maximum(items: EntityCachePoints): number | null {
    let max: number | null = null;
    items.forEach((item) => {
      if (item[1] !== null)
        if (max === null) max = item[1];
        else max = Math.max(item[1], max);
    });
    return max;
  }

  private _last(items: EntityCachePoints): number | null {
    if (items.length === 0) return null;
    return items.slice(-1)[0][1];
  }

  private _first(items: EntityCachePoints): number | null {
    if (items.length === 0) return null;
    return items[0][1];
  }

  private _median(items: EntityCachePoints) {
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const itemsDup = this._filterNulls([...items]).sort((a, b) => a[1]! - b[1]!);
    if (itemsDup.length === 0) return null;
    if (itemsDup.length === 1) return itemsDup[0][1];
    const mid = Math.floor((itemsDup.length - 1) / 2);
    if (itemsDup.length % 2 === 1) return itemsDup[mid][1];
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    return (itemsDup[mid][1]! + itemsDup[mid + 1][1]!) / 2;
  }

  private _delta(items: EntityCachePoints): number | null {
    const max = this._maximum(items);
    const min = this._minimum(items);
    return max === null || min === null ? null : max - min;
  }

  private _diff(items: EntityCachePoints): number | null {
    const noNulls = this._filterNulls(items);
    const first = this._first(noNulls);
    const last = this._last(noNulls);
    if (first === null || last === null) {
      return null;
    }
    return last - first;
  }

  private _filterNulls(items: EntityCachePoints): EntityCachePoints {
    return items.filter((item) => item[1] !== null);
  }
}
