

namespace FluentNest
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using Nest;

    public static class ExpressionFuncNestExtension
    {
        public static QueryContainer ValueWithin<T>(this Expression<Func<T, object>> propertyGetter, IEnumerable<string> list) where T : class
        {
            return new QueryContainerDescriptor<T>().AndValueWithin(propertyGetter, list);
        }

        public static QueryContainer ValueWithin<T>(this Expression<Func<T, object>> propertyGetter, string item) where T : class
        {
            return new QueryContainerDescriptor<T>().AndValueWithin(propertyGetter, item);
        }

    }
}
