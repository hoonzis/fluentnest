namespace FluentNest
{
    using System;
    using System.Linq.Expressions;
    using Nest;

    public static class Filters
    {
        public static QueryContainer CreateFilter<T>(Expression<Func<T, bool>> filterRule) where T : class
        {
            return filterRule.GenerateFilterDescription<T>();
        }
    }
}