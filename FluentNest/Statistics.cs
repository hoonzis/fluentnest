using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Nest;

namespace FluentNest
{
    public static class Statistics
    {
        static Func<AggregationContainerDescriptor<T>, AggregationContainerDescriptor<T>> GetAggregationFuncFromGetFieldNamed<T>(Expression<Func<T, object>> fieldGetter, AggType aggType) where T : class
        {
            var name = Names.GetNameFromGetFieldNamed(fieldGetter.Body);

            if (name == null)
            {
                return null;
            }

            var namedField = new Field(name);
            var aggName = fieldGetter.GetAggName(aggType);
            switch (aggType)
            {
                case AggType.Sum:
                    return x => x.Sum(aggName, field => field.Field(namedField));
                case AggType.Count:
                    return x => x.ValueCount(aggName, field => field.Field(namedField));
                case AggType.Average:
                    return x => x.Average(aggName, field => field.Field(namedField));
                case AggType.Cardinality:
                    return x => x.Cardinality(aggName, field => field.Field(namedField));
                case AggType.Stats:
                    return x => x.Stats(aggName, field => field.Field(namedField));
                case AggType.Max:
                    return x => x.Max(aggName, field => field.Field(namedField));
                case AggType.Min:
                    return x => x.Min(aggName, field => field.Field(namedField));
                case AggType.First:
                    return x => x.Terms(aggName, field => field.Field(namedField));
                case AggType.Percentile:
                    return x => x.Percentiles(aggName, field => field.Field(namedField));
            }

            throw new NotImplementedException();
        }

        private static Func<AggregationContainerDescriptor<T>, AggregationContainerDescriptor<T>>  GetAggregationFunc<T>(Expression<Func<T, object>> fieldGetter, AggType aggType) where T : class
        {
            var fromGetFieldNamed = GetAggregationFuncFromGetFieldNamed(fieldGetter, aggType);
            if (fromGetFieldNamed != null)
            {
                return fromGetFieldNamed;
            }

            var aggName = fieldGetter.GetAggName(aggType);
            switch (aggType)
            {
                case AggType.Sum:
                    return x => x.Sum(aggName, field => field.Field(fieldGetter));
                case AggType.Count:
                    return x => x.ValueCount(aggName, field => field.Field(fieldGetter));
                case AggType.Average:
                    return x => x.Average(aggName, field => field.Field(fieldGetter));
                case AggType.Cardinality:
                    return x => x.Cardinality(aggName, field => field.Field(fieldGetter));
                case AggType.Stats:
                    return x => x.Stats(aggName, field => field.Field(fieldGetter));
                case AggType.Max:
                    return x => x.Max(aggName, field => field.Field(fieldGetter));
                case AggType.Min:
                    return x => x.Min(aggName, field => field.Field(fieldGetter));
                case AggType.First:
                    return x => x.Terms(aggName, field => field.Field(fieldGetter));
                case AggType.Percentile:
                    return x => x.Percentiles(aggName, field => field.Field(fieldGetter));
            }

            throw new NotImplementedException();
        }

        private static AggregationContainerDescriptor<T> GetStatsDescriptor<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, AggType aggType, Expression<Func<T, bool>> filterRule = null) where T : class
        {
            var aggFunc = GetAggregationFunc(fieldGetter, aggType);

            if (filterRule == null)
            {
                return aggFunc(agg);
            }

            var filterName = filterRule.GenerateFilterName();
            agg.Filter(filterName, f => f.Filter(fd => filterRule.Body.GenerateFilterDescription<T>()).Aggregations(aggFunc));
            return agg;
        }

        public static AggregationContainerDescriptor<T> SumBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
        {
            return agg.GetStatsDescriptor(fieldGetter, AggType.Sum, filterRule);
        }

        public static AggregationContainerDescriptor<T> CountBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
        {
            return agg.GetStatsDescriptor(fieldGetter, AggType.Count, filterRule);
        }

        public static AggregationContainerDescriptor<T> CardinalityBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
        {
            return agg.GetStatsDescriptor(fieldGetter, AggType.Cardinality, filterRule);
        }

        public static AggregationContainerDescriptor<T> AverageBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
        {
            return agg.GetStatsDescriptor(fieldGetter, AggType.Average, filterRule);
        }

        public static AggregationContainerDescriptor<T> PercentilesBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
        {
            return agg.GetStatsDescriptor(fieldGetter, AggType.Percentile, filterRule);
        }

        public static AggregationContainerDescriptor<T> MaxBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
        {
            return agg.GetStatsDescriptor(fieldGetter, AggType.Max, filterRule);
        }

        public static AggregationContainerDescriptor<T> MinBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
        {
            return agg.GetStatsDescriptor(fieldGetter, AggType.Min, filterRule);
        }

        public static AggregationContainerDescriptor<T> StatsBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
        {
            return agg.GetStatsDescriptor(fieldGetter, AggType.Stats, filterRule);
        }

        public static AggregationContainerDescriptor<T> FirstBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter, Expression<Func<T, bool>> filterRule = null) where T : class
        {
            return agg.GetStatsDescriptor(fieldGetter, AggType.First, filterRule);
        }

        public static AggregationContainerDescriptor<T> DistinctBy<T>(this AggregationContainerDescriptor<T> agg, Expression<Func<T, object>> fieldGetter) where T : class
        {
            var aggName = fieldGetter.GetAggName(AggType.Distinct);
            return agg.Terms(aggName, x => x.Field(fieldGetter).Size(int.MaxValue));
        }

        public static AggregationContainerDescriptor<T> TopHits<T>(this AggregationContainerDescriptor<T> agg, int size, params Expression<Func<T, object>>[] fieldGetter) where T : class
        {
            var aggName = AggType.TopHits.ToString();
            return agg.TopHits(aggName, x => x.Size(size).Source(i=>i.Includes(f=>f.Fields(fieldGetter))));
        }

        private class PromiseValue<T> : IPromise<T> where T : class
        {
            private readonly T value;

            public PromiseValue(T value)
            {
                this.value = value;
            }

            T IPromise<T>.Value => value;
        }

        public static AggregationContainerDescriptor<T> SortedTopHits<T>(this AggregationContainerDescriptor<T> agg, int size, Expression<Func<T, object>> fieldSort,SortType sorttype, params Expression<Func<T, object>>[] fieldGetter) where T : class
        {
            var aggName = sorttype + fieldSort.GetAggName(AggType.TopHits);
            var sortFieldDescriptor = new SortFieldDescriptor<T>();
            var fieldSortName = Names.GetNameFromGetFieldNamed(fieldSort.Body);
            sortFieldDescriptor = fieldSortName != null ? sortFieldDescriptor.Field(fieldSortName) : sortFieldDescriptor.Field(fieldSort);
            sortFieldDescriptor = sorttype == SortType.Ascending ? sortFieldDescriptor.Ascending() : sortFieldDescriptor.Descending();

            var fieldNames = fieldGetter.Select(x => Names.GetNameFromGetFieldNamed(x.Body)).Where(x => x != null);
            var fieldGetters = fieldGetter.Where(x => Names.GetNameFromGetFieldNamed(x.Body) == null);

            var allFields = fieldNames.Select(x => new Field(x)).Concat(fieldGetters.Select(x => new Field(x)));

            return agg.TopHits(
                aggName,
                x =>
                    x
                        .Size(size)
                        .Source(i => i.Includes(f=>f.Fields(allFields)))
                        .Sort(s => new PromiseValue<IList<ISort>>(new List<ISort> {sortFieldDescriptor})));
        }
    }
}
