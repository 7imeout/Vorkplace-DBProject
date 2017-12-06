/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Database;
import org.h2.message.DbException;
import org.h2.util.ValueHashMap;
import org.h2.value.*;

/**
 * Data stored while calculating an aggregate.
 */
class AggregateDataDefault extends AggregateData {
    private final int aggregateType;
    private long count;
    private ValueHashMap<AggregateDataDefault> distinctValues;
    private Value value;
    private double m2, mean;
    private double product, sum;

    /**
     * @param aggregateType the type of the aggregate operation
     */
    AggregateDataDefault(int aggregateType) {
        this.aggregateType = aggregateType;
    }

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        count++;
        if (distinct) {
            if (distinctValues == null) {
                distinctValues = ValueHashMap.newInstance();
            }
            distinctValues.put(v, this);
            return;
        }
        switch (aggregateType) {
        case Aggregate.SUM:
            if (value == null) {
                value = v.convertTo(dataType);
            } else {
                v = v.convertTo(value.getType());
                value = value.add(v);
            }
            break;
        case Aggregate.AVG:
            if (value == null) {
                value = v.convertTo(DataType.getAddProofType(dataType));
            } else {
                v = v.convertTo(value.getType());
                value = value.add(v);
            }
            break;
        case Aggregate.MIN:
            if (value == null || database.compare(v, value) < 0) {
                value = v;
            }
            break;
        case Aggregate.MAX:
            if (value == null || database.compare(v, value) > 0) {
                value = v;
            }
            break;
        case Aggregate.STDDEV_POP:
        case Aggregate.STDDEV_SAMP:
        case Aggregate.VAR_POP:
        case Aggregate.VAR_SAMP: {
            // Using Welford's method, see also
            // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
            // http://www.johndcook.com/standard_deviation.html
            double x = v.getDouble();
            if (count == 1) {
                mean = x;
                m2 = 0;
            } else {
                double delta = x - mean;
                mean += delta / count;
                m2 += delta * (x - mean);
            }
            break;
        }
        case Aggregate.BOOL_AND:
            v = v.convertTo(Value.BOOLEAN);
            if (value == null) {
                value = v;
            } else {
                value = ValueBoolean.get(value.getBoolean().booleanValue() &&
                        v.getBoolean().booleanValue());
            }
            break;
        case Aggregate.BOOL_OR:
            v = v.convertTo(Value.BOOLEAN);
            if (value == null) {
                value = v;
            } else {
                value = ValueBoolean.get(value.getBoolean().booleanValue() ||
                        v.getBoolean().booleanValue());
            }
            break;
        case Aggregate.BIT_AND:
            if (value == null) {
                value = v.convertTo(dataType);
            } else {
                value = ValueLong.get(value.getLong() & v.getLong()).convertTo(dataType);
            }
            break;
        case Aggregate.BIT_OR:
            if (value == null) {
                value = v.convertTo(dataType);
            } else {
                value = ValueLong.get(value.getLong() | v.getLong()).convertTo(dataType);
            }
            break;
        case Aggregate.CONF_INT_80:
        case Aggregate.CONF_INT_85:
        case Aggregate.CONF_INT_90:
        case Aggregate.CONF_INT_95:
        case Aggregate.CONF_INT_99:
        case Aggregate.CONF_INT_99_5:
        case Aggregate.CONF_INT_99_9:
            double x = v.getDouble();
            if (count == 1) {
                mean = x;
                m2 = 0;
            } else {
                double delta = x - mean;
                mean += delta / count;
                m2 += delta * (x - mean);
            }
            break;
        case Aggregate.GEOMEAN:
            double y = v.getDouble();
            if (count == 1) {
                product = y;
            }
            else {
                product *= y;
            }
            break;
        case Aggregate.HARMEAN:
            double z = v.getDouble();
            if (count == 1) {
                sum = (1.0 / z);
            }
            else {
                sum += (1.0 / z);
            }
            break;
        default:
            DbException.throwInternalError("type=" + aggregateType);
        }
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        if (distinct) {
            count = 0;
            groupDistinct(database, dataType);
        }
        Value v = null;
        switch (aggregateType) {
        case Aggregate.SUM:
        case Aggregate.MIN:
        case Aggregate.MAX:
        case Aggregate.BIT_OR:
        case Aggregate.BIT_AND:
        case Aggregate.BOOL_OR:
        case Aggregate.BOOL_AND:
            v = value;
            break;
        case Aggregate.AVG:
            if (value != null) {
                v = divide(value, count);
            }
            break;
        case Aggregate.STDDEV_POP: {
            if (count < 1) {
                return ValueNull.INSTANCE;
            }
            v = ValueDouble.get(Math.sqrt(m2 / count));
            break;
        }
        case Aggregate.STDDEV_SAMP: {
            if (count < 2) {
                return ValueNull.INSTANCE;
            }
            v = ValueDouble.get(Math.sqrt(m2 / (count - 1)));
            break;
        }
        case Aggregate.VAR_POP: {
            if (count < 1) {
                return ValueNull.INSTANCE;
            }
            v = ValueDouble.get(m2 / count);
            break;
        }
        case Aggregate.VAR_SAMP: {
            if (count < 2) {
                return ValueNull.INSTANCE;
            }
            v = ValueDouble.get(m2 / (count - 1));
            break;
        }
        case Aggregate.CONF_INT_80: {
            if (count == 0) {
                return ValueString.get("null");
            }
            double stdDev = Math.sqrt(m2 / (count - 1));
            double left = mean - (1.282 * (stdDev / Math.sqrt(count)));
            double right = mean + (1.282 * (stdDev / Math.sqrt(count)));
            String bottom = String.valueOf((double)Math.round(left * 1000d) / 1000d);
            String top = String.valueOf((double)Math.round(right * 1000d) / 1000d);
            String result = "(" + bottom + ", " + top + ")";
            v = ValueString.get(result);
            break;
        }
        case Aggregate.CONF_INT_85: {
            if (count == 0) {
                return ValueString.get("null");
            }
            double stdDev = Math.sqrt(m2 / (count - 1));
            double left = mean - (1.440 * (stdDev / Math.sqrt(count)));
            double right = mean + (1.440 * (stdDev / Math.sqrt(count)));
            String bottom = String.valueOf((double)Math.round(left * 1000d) / 1000d);
            String top = String.valueOf((double)Math.round(right * 1000d) / 1000d);
            String result = "(" + bottom + ", " + top + ")";
            v = ValueString.get(result);
            break;
        }
        case Aggregate.CONF_INT_90: {
            if (count == 0) {
                return ValueString.get("null");
            }
            double stdDev = Math.sqrt(m2 / (count - 1));
            double left = mean - (1.645 * (stdDev / Math.sqrt(count)));
            double right = mean + (1.645 * (stdDev / Math.sqrt(count)));
            String bottom = String.valueOf((double)Math.round(left * 1000d) / 1000d);
            String top = String.valueOf((double)Math.round(right * 1000d) / 1000d);
            String result = "(" + bottom + ", " + top + ")";
            v = ValueString.get(result);
            break;
        }
        case Aggregate.CONF_INT_95: {
            if (count == 0) {
                return ValueString.get("null");
            }
            double stdDev = Math.sqrt(m2 / (count - 1));
            double left = mean - (1.960 * (stdDev / Math.sqrt(count)));
            double right = mean + (1.960 * (stdDev / Math.sqrt(count)));
            String bottom = String.valueOf((double)Math.round(left * 1000d) / 1000d);
            String top = String.valueOf((double)Math.round(right * 1000d) / 1000d);
            String result = "(" + bottom + ", " + top + ")";
            v = ValueString.get(result);
            break;
        }
        case Aggregate.CONF_INT_99: {
            if (count == 0) {
                return ValueString.get("null");
            }
            double stdDev = Math.sqrt(m2 / (count - 1));
            double left = mean - (2.576 * (stdDev / Math.sqrt(count)));
            double right = mean + (2.576 * (stdDev / Math.sqrt(count)));
            String bottom = String.valueOf((double)Math.round(left * 1000d) / 1000d);
            String top = String.valueOf((double)Math.round(right * 1000d) / 1000d);
            String result = "(" + bottom + ", " + top + ")";
            v = ValueString.get(result);
            break;
        }
        case Aggregate.CONF_INT_99_5: {
            if (count == 0) {
                return ValueString.get("null");
            }
            double stdDev = Math.sqrt(m2 / (count - 1));
            double left = mean - (2.807 * (stdDev / Math.sqrt(count)));
            double right = mean + (2.807 * (stdDev / Math.sqrt(count)));
            String bottom = String.valueOf((double)Math.round(left * 1000d) / 1000d);
            String top = String.valueOf((double)Math.round(right * 1000d) / 1000d);
            String result = "(" + bottom + ", " + top + ")";
            v = ValueString.get(result);
            break;
        }
        case Aggregate.CONF_INT_99_9: {
            if (count == 0) {
                return ValueString.get("null");
            }
            double stdDev = Math.sqrt(m2 / (count - 1));
            double left = mean - (3.291 * (stdDev / Math.sqrt(count)));
            double right = mean + (3.291 * (stdDev / Math.sqrt(count)));
            String bottom = String.valueOf((double)Math.round(left * 1000d) / 1000d);
            String top = String.valueOf((double)Math.round(right * 1000d) / 1000d);
            String result = "(" + bottom + ", " + top + ")";
            v = ValueString.get(result);
            break;
        }
        case Aggregate.GEOMEAN: {
            if (count == 0) {
                return ValueString.get("null");
            }
            double geomean = Math.pow(product, (1.0 / count));
            geomean = (double)Math.round(geomean * 1000d) / 1000d;
            v = ValueDouble.get(geomean);
            break;
        }
        case Aggregate.HARMEAN: {
            if (count == 0) {
                return ValueString.get("null");
            }
            double harmean = count / sum;
            harmean = (double)Math.round(harmean * 1000d) / 1000d;
            v = ValueDouble.get(harmean);
            break;
        }
        default:
            DbException.throwInternalError("type=" + aggregateType);
        }
        return v == null ? ValueNull.INSTANCE : v.convertTo(dataType);
    }

    private static Value divide(Value a, long by) {
        if (by == 0) {
            return ValueNull.INSTANCE;
        }
        int type = Value.getHigherOrder(a.getType(), Value.LONG);
        Value b = ValueLong.get(by).convertTo(type);
        a = a.convertTo(type).divide(b);
        return a;
    }

    private void groupDistinct(Database database, int dataType) {
        if (distinctValues == null) {
            return;
        }
        count = 0;
        for (Value v : distinctValues.keys()) {
            add(database, dataType, false, v);
        }
    }

}
