namespace FluentNest
{
    using System;
    using System.Linq.Expressions;

    public static class BinaryExpressionExtension
    {
        public static object GetValue(this BinaryExpression binaryExpression)
        {
            var leftHand = binaryExpression.Left;
            var valueExpression = binaryExpression.Right;

            if (leftHand is UnaryExpression)
            {
                // This is necessary in order to avoid the automatic cast of enums to the underlying integer representation
                // In some cases the lambda comes in the shape (Convert(EngineType), 0), where 0 represents the first case of the EngineType enum
                // In such cases, we don't want the value in the Terms to be 0, but rather we pass the enum value (e.g. EngineType.Diesel)
                // and we let the serializer to do it's job and spit out Term("fieldName","diesel") or Term("fieldName","0") depending whether it is converting enums as integers or strings
                // or anything else
                var unaryExpression = leftHand as UnaryExpression;
                var operandType = unaryExpression.Operand.Type;
                var underlyingNullableType = Nullable.GetUnderlyingType(operandType);
                var typeToConsider = underlyingNullableType != null ? underlyingNullableType : operandType;
                if (typeToConsider.IsEnum)
                {
                    valueExpression = Expression.Convert(binaryExpression.Right, operandType);
                }
            }

            var objectMember = Expression.Convert(valueExpression, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter();

        }
        
    }
}
