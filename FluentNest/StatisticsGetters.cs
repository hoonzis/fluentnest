using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Nest;

namespace FluentNest
{
    public static class StatisticsGetters
    {

        /// <summary>
        /// Takes a value metric and forces a conversion to certain type
        /// </summary>
        private static TK ValueAsUndType<TK>(ValueAggregate agg)
        {
            var type = typeof(TK);
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var undType = Nullable.GetUnderlyingType(type);
                if (!agg.Value.HasValue)
                {
                    return (TK)(object)null;
                }
                var valueAsUndType = Convert.ChangeType(agg.Value, undType);
                return (TK)(object)valueAsUndType;
            }

            // seems that by default ES stores the datetime value as unix timestamp in miliseconds
            if (typeof (TK) == typeof (DateTime) && agg.Value.HasValue)
            {
                DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0);
                return (TK)(object)origin.AddMilliseconds(agg.Value.Value);
            }
            return (TK)Convert.ChangeType(agg.Value, typeof(TK));

        }

        public static AggregationsHelper GetAggregationContainingResult<T>(this AggregationsHelper aggs,
            Expression<Func<T, object>> filterRule = null)
        {
            if (filterRule == null)
            {
                return aggs;
            }

            var filterName = filterRule.GenerateFilterName();
            aggs.CheckForAggregationInResult(filterName);
            return aggs.Filter(filterName);
        }

        public static int GetCardinality<T>(this AggregationsHelper aggs, Expression<Func<T, object>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            var aggWithResult = GetAggregationContainingResult(aggs, filterRule);
            var aggName = fieldGetter.GetAggName(AggType.Cardinality);
            aggWithResult.CheckForAggregationInResult(aggName);
            var itemsTerms = aggWithResult.Cardinality(aggName);
            return (int)itemsTerms.Value.Value;
        }

        public static TK GetSum<T,TK>(this AggregationsHelper aggs, Expression<Func<T, TK>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            var aggWithResult = GetAggregationContainingResult(aggs, filterRule);
            var aggName = fieldGetter.GetAggName(AggType.Sum);
            aggWithResult.CheckForAggregationInResult(aggName);
            var sumAgg = aggWithResult.Sum(aggName);
            return ValueAsUndType<TK>(sumAgg);
        }

        public static TK GetFirstBy<T,TK>(this AggregationsHelper aggs, Expression<Func<T, TK>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            var aggWithResult = GetAggregationContainingResult(aggs, filterRule);
            var aggName = fieldGetter.GetAggName(AggType.First);
            aggWithResult.CheckForAggregationInResult(aggName);
            var termsAgg = aggWithResult.Terms(aggName);
            return Filters.StringToAnything<TK>(termsAgg.Buckets.First().Key);
        }

        public static TK GetAverage<T,TK>(this AggregationsHelper aggs, Expression<Func<T, TK>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            var aggWithResult = GetAggregationContainingResult(aggs, filterRule);
            var aggName = fieldGetter.GetAggName(AggType.Average);
            aggWithResult.CheckForAggregationInResult(aggName);
            var avgAgg = aggWithResult.Average(aggName);
            return ValueAsUndType<TK>(avgAgg);
        }

        public static TK GetMin<T,TK>(this AggregationsHelper aggs, Expression<Func<T, TK>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            var aggWithResult = GetAggregationContainingResult(aggs, filterRule);
            var aggName = fieldGetter.GetAggName(AggType.Min);
            aggWithResult.CheckForAggregationInResult(aggName);
            var minAgg = aggWithResult.Min(aggName);
            return ValueAsUndType<TK>(minAgg);
        }

        public static TK GetMax<T,TK>(this AggregationsHelper aggs, Expression<Func<T, TK>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            var aggWithResult = GetAggregationContainingResult(aggs, filterRule);
            var aggName = fieldGetter.GetAggName(AggType.Max);
            aggWithResult.CheckForAggregationInResult(aggName);
            var maxAgg = aggWithResult.Max(aggName);
            return ValueAsUndType<TK>(maxAgg);
        }

        public static IList<PercentileItem> GetPercentile<T>(this AggregationsHelper aggs, Expression<Func<T, object>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            var aggWithResult = GetAggregationContainingResult(aggs, filterRule);
            var aggName = fieldGetter.GetAggName(AggType.Percentile);
            aggWithResult.CheckForAggregationInResult(aggName);
            var itemsTerms = aggWithResult.Percentiles(aggName);
            return itemsTerms.Items;
        }

        public static StatsAggregate GetStats<T>(this AggregationsHelper aggs, Expression<Func<T, object>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            var aggWithResult = GetAggregationContainingResult(aggs, filterRule);
            var aggName = fieldGetter.GetAggName(AggType.Stats);
            aggWithResult.CheckForAggregationInResult(aggName);
            var itemsTerms = aggWithResult.Stats(aggName);
            return itemsTerms;
        }

        public static int? GetCount<T>(this AggregationsHelper aggs, Expression<Func<T, object>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            var aggWithResult = GetAggregationContainingResult(aggs, filterRule);
            var aggName = fieldGetter.GetAggName(AggType.Count);
            aggWithResult.CheckForAggregationInResult(aggName);
            var itemsTerms = aggWithResult.ValueCount(aggName);
            if (!itemsTerms.Value.HasValue)
                return null;
            return (int)itemsTerms.Value;
        }

        public static IEnumerable<V> GetDistinct<T, V>(this AggregationsHelper aggs, Expression<Func<T, V>> fieldGetter)
        {
            var aggName = fieldGetter.GetAggName(AggType.Distinct);
            var itemsTerms = aggs.Terms(aggName);
            var targetType = typeof(V);
            if (targetType.IsEnum)
            {
                return itemsTerms.Buckets.Select((x => Filters.Parse<V>(x.Key)));
            }

            if (targetType == typeof(string))
            {
                return itemsTerms.Buckets.Select(x => x.Key).Cast<V>();
            }

            if (targetType == typeof(long) || targetType == typeof(int))
            {
                return itemsTerms.Buckets.Select(x => long.Parse(x.Key)).Cast<V>();
            }

            throw new NotImplementedException("You can get only distinct values of Strings, Enums, ints or long");
        }

        public static IEnumerable<T> GetTopHits<T>(this AggregationsHelper aggs) where T:class 
        {
            var topHits = aggs.TopHits(AggType.TopHits.ToString());
            return topHits.Hits<T>().Select(x => x.Source);
        }

        public static IEnumerable<T> GetSortedTopHits<T>(this AggregationsHelper aggs, Expression<Func<T, object>> sorter, SortType sortType) where T : class
        {
            var aggName = sortType + sorter.GetAggName(AggType.TopHits);
            var topHits = aggs.TopHits(aggName);
            return topHits.Hits<T>().Select(x => x.Source);
        }
    }
}