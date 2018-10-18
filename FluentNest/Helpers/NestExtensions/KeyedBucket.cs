

namespace FluentNest
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using Nest;

    public static class KeyedBucketNestExtension
    {
        public static IReadOnlyCollection<DateHistogramBucket> GetDateHistogram<T>(this KeyedBucket<T> item,
            Expression<Func<T, Object>> fieldGetter)
        {
            var histogramItem = item.DateHistogram(fieldGetter.GetName());
            return histogramItem.Buckets;
        }

    }
}
