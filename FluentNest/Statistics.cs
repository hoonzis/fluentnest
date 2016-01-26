using System;
using System.Linq.Expressions;
using Nest;

namespace FluentNest
{
    public static class Statistics
    {
        public static AggregationDescriptor<T> SumBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Sum);
            if (filterRule == null)
            {
                return agg.Sum(aggName, x => x.Field(fieldGetter));
            }
            
            var filterName = filterRule.GenerateFilterName();
            agg.Filter(filterName,
                f =>
                    f.Filter(fd => filterRule.Body.GenerateFilterDescription<T>())
                        .Aggregations(innerAgg => innerAgg.Sum(aggName, field => field.Field(fieldGetter))));
            return agg;
        }

        public static AggregationDescriptor<T> FirstBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Sum);
            if (filterRule == null)
            {
                return agg.Terms(aggName, x => x.Field(fieldGetter));
            }

            var filterName = filterRule.GenerateFilterName();
            agg.Filter(filterName,
                f =>
                    f.Filter(fd => filterRule.Body.GenerateFilterDescription<T>())
                        .Aggregations(innerAgg => innerAgg.Terms(aggName, field => field.Field(fieldGetter))));
            return agg;
        }

        public static AggregationDescriptor<T> CountBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Count);
            if (filterRule == null)
            {
                return agg.ValueCount(aggName, x => x.Field(fieldGetter));
            }
            
            var filterName = filterRule.GenerateFilterName();
            agg.Filter(filterName,
                f =>
                    f.Filter(fd => filterRule.Body.GenerateFilterDescription<T>())
                        .Aggregations(innerAgg => innerAgg.ValueCount(aggName, field => field.Field(fieldGetter))));
            return agg;
        }

        public static AggregationDescriptor<T> CardinalityBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, object>> fieldGetter) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Cardinality);
            return agg.Cardinality(aggName, x => x.Field(fieldGetter));
        }

        public static AggregationDescriptor<T> DistinctBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, object>> fieldGetter) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Distinct);
            return agg.Terms(aggName, x => x.Field(fieldGetter));
        }

        public static AggregationDescriptor<T> AverageBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, object>> fieldGetter) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Average);
            return agg.Average(aggName, x => x.Field(fieldGetter));
        }

        public static AggregationDescriptor<T> PercentilesBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, object>> fieldGetter) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Percentile);
            return agg.Percentiles(aggName, x => x.Field(fieldGetter));
        }

        public static AggregationDescriptor<T> MaxBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, object>> fieldGetter) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Max);
            return agg.Max(aggName, x => x.Field(fieldGetter));
        }

        public static AggregationDescriptor<T> MinBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, object>> fieldGetter) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Min);
            return agg.Min(aggName, x => x.Field(fieldGetter));
        }

        public static AggregationDescriptor<T> StatsBy<T>(this AggregationDescriptor<T> agg, Expression<Func<T, object>> fieldGetter) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Stats);
            return agg.Stats(aggName, x => x.Field(fieldGetter));
        }
    }
}
