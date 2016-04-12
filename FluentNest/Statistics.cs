using System;
using System.Linq.Expressions;
using Nest;

namespace FluentNest
{
    public static class Statistics
    {
        public static AggregationContainerDescriptor<T> SumBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
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

        public static AggregationContainerDescriptor<T> FirstBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
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

        public static AggregationContainerDescriptor<T> CountBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
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

        public static AggregationContainerDescriptor<T> CardinalityBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Cardinality);

            if (filterRule == null)
            {
                return agg.Cardinality(aggName, x => x.Field(fieldGetter));
            }

            var filterName = filterRule.GenerateFilterName();
            agg.Filter(filterName,
                f =>
                    f.Filter(fd => filterRule.Body.GenerateFilterDescription<T>())
                        .Aggregations(innerAgg => innerAgg.Cardinality(aggName, field => field.Field(fieldGetter))));

            return agg;
        }

        public static AggregationContainerDescriptor<T> DistinctBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Distinct);
            return agg.Terms(aggName, x => x.Field(fieldGetter).Size(int.MaxValue));
        }

        public static AggregationContainerDescriptor<T> AverageBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Average);
            return agg.Average(aggName, x => x.Field(fieldGetter));
        }

        public static AggregationContainerDescriptor<T> PercentilesBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Percentile);
            return agg.Percentiles(aggName, x => x.Field(fieldGetter));
        }

        public static AggregationContainerDescriptor<T> MaxBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Max);
            return agg.Max(aggName, x => x.Field(fieldGetter));
        }

        public static AggregationContainerDescriptor<T> MinBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Min);
            return agg.Min(aggName, x => x.Field(fieldGetter));
        }

        public static AggregationContainerDescriptor<T> StatsBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Stats);
            return agg.Stats(aggName, x => x.Field(fieldGetter));
        }
    }
}
