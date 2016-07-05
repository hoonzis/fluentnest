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
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="K"></typeparam>
        /// <param name="agg"></param>
        /// <returns></returns>
        private static K ValueAsUndType<K>(ValueMetric agg)
        {
            var type = typeof(K);
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var undType = Nullable.GetUnderlyingType(type);
                if (!agg.Value.HasValue)
                {
                    return (K)(Object)null;
                }
                var valueAsUndType = Convert.ChangeType(agg.Value, undType);
                return (K)(Object)valueAsUndType;
            }

            //seems that by default ES stores the datetime value as unix timestamp in miliseconds
            else if (typeof (K) == typeof (DateTime) && agg.Value.HasValue)
            {
                DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0);
                return (K)(Object)origin.AddMilliseconds(agg.Value.Value);
            }
            else
            {
                return (K)Convert.ChangeType(agg.Value, typeof(K));
            }
        }

        public static int GetCardinality<T>(this AggregationsHelper aggs, Expression<Func<T, object>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            var aggName = fieldGetter.GetAggName(AggType.Cardinality);
            ValueMetric itemsTerms;

            if (filterRule == null)
            {
                itemsTerms = aggs.Cardinality(aggName);
            }
            else
            {
                var filterName = filterRule.GenerateFilterName();
                var filterAgg = aggs.Filter(filterName);
                itemsTerms = filterAgg.Cardinality(aggName);
            }

            if (itemsTerms?.Value == null)
                throw new InvalidOperationException("Cardinality not available");

            return (int)itemsTerms.Value.Value;
        }

        public static K GetSum<T,K>(this AggregationsHelper aggs, Expression<Func<T, K>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            var aggName = fieldGetter.GetAggName(AggType.Sum);
            if (filterRule == null)
            {
                var sumAggs = aggs.Sum(aggName);
                return ValueAsUndType<K>(sumAggs);
            }
            else
            {
                var filterName = filterRule.GenerateFilterName();
                var filterAgg = aggs.Filter(filterName);
                var sumAgg = filterAgg.Sum(aggName);

                return ValueAsUndType<K>(sumAgg);
            }
        }

        public static K GetFirstBy<T,K>(this AggregationsHelper aggs, Expression<Func<T, K>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            var aggName = fieldGetter.GetAggName(AggType.Sum);
            if (filterRule == null)
            {
                var terms = aggs.Terms(aggName);
                return Filters.StringToAnything<K>(terms.Items[0].Key);
            }
            else
            {
                var filterName = filterRule.GenerateFilterName();
                var filterAgg = aggs.Filter(filterName);
                var termsAgg = filterAgg.Terms(aggName);
                return Filters.StringToAnything<K>(termsAgg.Items[0].Key);
            }
        }

        public static K GetAverage<T,K>(this AggregationsHelper aggs, Expression<Func<T, K>> fieldGetter)
        {
            var aggName = fieldGetter.GetAggName(AggType.Average);
            var avgAgg = aggs.Average(aggName);
            return ValueAsUndType<K>(avgAgg);
        }

        public static K GetMin<T,K>(this AggregationsHelper aggs, Expression<Func<T, K>> fieldGetter)
        {
            var aggName = fieldGetter.GetAggName(AggType.Min);
            var minAgg = aggs.Min(aggName);
            return ValueAsUndType<K>(minAgg);
        }

        public static K GetMax<T,K>(this AggregationsHelper aggs, Expression<Func<T, K>> fieldGetter)
        {
            var aggName = fieldGetter.GetAggName(AggType.Max);
            var maxAgg = aggs.Max(aggName);
            return ValueAsUndType<K>(maxAgg);
        }

        public static IList<PercentileItem> GetPercentile<T>(this AggregationsHelper aggs, Expression<Func<T, object>> fieldGetter)
        {
            var aggName = fieldGetter.GetAggName(AggType.Percentile);
            var itemsTerms = aggs.Percentiles(aggName);
            return itemsTerms.Items;
        }

        public static StatsMetric GetStats<T>(this AggregationsHelper aggs, Expression<Func<T, object>> fieldGetter)
        {
            var aggName = fieldGetter.GetAggName(AggType.Stats);
            var itemsTerms = aggs.Stats(aggName);
            return itemsTerms;
        }

        public static int? GetCount<T>(this AggregationsHelper aggs, Expression<Func<T, object>> fieldGetter, Expression<Func<T, object>> filterRule = null)
        {
            var aggName = fieldGetter.GetAggName(AggType.Count);
            if (filterRule == null)
            {
                var itemsTerms = aggs.ValueCount(aggName);
                if (!itemsTerms.Value.HasValue)
                    return null;
                return (int)itemsTerms.Value;
            }
            else
            {
                var condAggName = filterRule.GenerateFilterName();
                var filterAgg = aggs.Filter(condAggName);
                var sumAgg = filterAgg.Sum(aggName);
                if (!sumAgg.Value.HasValue)
                    return null;
                return (int)sumAgg.Value;
            }
        }

        public static IEnumerable<V> GetDistinct<T, V>(this AggregationsHelper aggs, Expression<Func<T, V>> fieldGetter)
        {
            var aggName = fieldGetter.GetAggName(AggType.Distinct);
            var itemsTerms = aggs.Terms(aggName);
            var targetType = typeof (V);
            if (targetType.IsEnum)
            {
                return itemsTerms.Items.Select((x => Filters.Parse<V>(x.Key)));
            }

            if (targetType == typeof(string))
            {
                return itemsTerms.Items.Select(x => x.Key).Cast<V>();
            }

            if (targetType == typeof(long) || targetType == typeof(int))
            {
                return itemsTerms.Items.Select(x => long.Parse(x.Key)).Cast<V>();
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