
namespace FluentNest
{
    using System.Linq.Expressions;
    using Nest;

    public static class BinaryExpressionNestExtension
    {
        public static QueryContainer GenerateEqualityFilter<T>(this BinaryExpression binaryExpression) where T : class
        {
            var value = binaryExpression.GetValue();
            if (value == null)
            {
                return GenerateNonExistenceFilter<T>(binaryExpression);
            }

            var queryContainerDescriptor = new QueryContainerDescriptor<T>();
            var fieldExpression = binaryExpression.Left.GetFieldExpression<T>();
            return fieldExpression.Expression != null
                ? queryContainerDescriptor.Term(fieldExpression.Expression, value)
                : queryContainerDescriptor.Term(fieldExpression.Field, value);
        }

       
        public static QueryContainer GenerateNotEqualFilter<T>(this BinaryExpression expression) where T : class
        {
            var equalityFilter = GenerateEqualityFilter<T>(expression);
            var filterDescriptor = new QueryContainerDescriptor<T>();
            return filterDescriptor.Bool(x => x.MustNot(equalityFilter));
        }

        private static QueryContainer GenerateNonExistenceFilter<T>(BinaryExpression binaryExpression) where T : class
        {
            var queryContainerDescriptor = new QueryContainerDescriptor<T>();
            var fieldExpression = binaryExpression.Left.GetFieldExpression<T>();
            return queryContainerDescriptor.Bool(b => b.MustNot(m => m.Exists(e => e.Field(fieldExpression.Expression ?? fieldExpression.Field))));
        }
       
    }
}
