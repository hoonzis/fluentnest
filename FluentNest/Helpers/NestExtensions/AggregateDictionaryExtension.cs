

namespace FluentNest
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using Nest;

    public static class AggregateDictionaryNestExtension
    {
        public static AggsContainer<T> AsContainer<T>(this AggregateDictionary aggs)
        {
            return new AggsContainer<T>(aggs);
        }

        public static IReadOnlyCollection<DateHistogramBucket> GetDateHistogram<T>(this AggregateDictionary aggs,
            Expression<Func<T, Object>> fieldGetter)
        {
            var histogramItem = aggs.DateHistogram(fieldGetter.GetName());
            return histogramItem.Buckets;
        }

        public static IReadOnlyCollection<KeyedBucket<double>> GetHistogram<T>(this AggregateDictionary aggs,
            Expression<Func<T, Object>> fieldGetter)
        {
            var histogramItem = aggs.Histogram(fieldGetter.GetName());
            return histogramItem.Buckets;
        }
    }
}
