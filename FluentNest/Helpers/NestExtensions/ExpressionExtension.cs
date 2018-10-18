using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Nest;

namespace FluentNest
{
    public static class ExpressionNestExtension
    {

        public static QueryContainer GenerateBoolFilter<T>(this Expression expression) where T : class
        {
            var filterDescriptor = new QueryContainerDescriptor<T>();
            var fieldName = expression.GenerateFilterName();
            return filterDescriptor.Term(fieldName, true);
        }

        public static QueryContainer GenerateComparisonFilter<T>(this Expression expression, ExpressionType type)
            where T : class
        {
            var binaryExpression = (BinaryExpression) expression;

            var value = binaryExpression.GetValue();

            if (value == null)
            {
                // if the value is null, no filters are added
                return new QueryContainer();
            }

            var fieldName = binaryExpression.Left.GetFieldNameFromMemberOrGetFieldNamed();
            var filterDescriptor = new QueryContainerDescriptor<T>();

            switch (value)
            {
                case DateTime time:
                    return filterDescriptor.DateRange(x => x.RangeOnDate(type, time).Field(fieldName));
                case double _:
                case decimal _:
                    return filterDescriptor.Range(x => x.RangeOnNumber(type, Convert.ToDouble(value)).Field(fieldName));
                case int _:
                case long _:
                    return filterDescriptor.Range(x => x.RangeOnNumber(type, Convert.ToInt64(value)).Field(fieldName));
            }

            throw new InvalidOperationException("Comparison on non-supported type");
        }
    }
}
