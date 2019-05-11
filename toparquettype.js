// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

const { Type, Visitor } = require('apache-arrow');

/** @ignore */ const unsupportedParquetTypeErrorMessage = (typeId) => `Unsupported Arrow to Parquet type "${Type[typeId]}"`;

class GetParquetTypeVisitor extends Visitor {
    visitNull                 (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitBool                 (_ype) { return { type: 'BOOLEAN' };                                           }
    visitInt8                 (_ype) { return { type: 'INT_8' };                                             }
    visitInt16                (_ype) { return { type: 'INT_16' };                                            }
    visitInt32                (_ype) { return { type: 'INT_32' };                                            }
    visitInt64                (_ype) { return { type: 'INT_64' };                                            }
    visitUint8                (_ype) { return { type: 'UINT_8' };                                            }
    visitUint16               (_ype) { return { type: 'UINT_16' };                                           }
    visitUint32               (_ype) { return { type: 'UINT_32' };                                           }
    visitUint64               (_ype) { return { type: 'UINT_64' };                                           }
    visitFloat16              (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitFloat32              (_ype) { return { type: 'FLOAT' };                                             }
    visitFloat64              (_ype) { return { type: 'DOUBLE' };                                            }
    visitUtf8                 (_ype) { return { type: 'UTF8' };                                              }
    visitBinary               (_ype) { return { type: 'BYTE_ARRAY' };                                        }
    visitFixedSizeBinary      (_ype) { return { type: 'BYTE_ARRAY' };                                        }
    visitDate                 (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitDateDay              (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitDateMillisecond      (_ype) { return { type: 'TIMESTAMP_MILLIS' };                                  }
    visitTimestamp            (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitTimestampSecond      (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitTimestampMillisecond (_ype) { return { type: 'TIMESTAMP_MILLIS' };                                  }
    visitTimestampMicrosecond (_ype) { return { type: 'TIMESTAMP_MICROS' };                                  }
    visitTimestampNanosecond  (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitTime                 (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitTimeSecond           (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitTimeMillisecond      (_ype) { return { type: 'TIME_MILLIS' };                                       }
    visitTimeMicrosecond      (_ype) { return { type: 'TIME_MICROS' };                                       }
    visitTimeNanosecond       (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitDecimal              (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitList                 (_ype) { return { type: 'JSON' };                                              }
    visitStruct               (_ype) { return { repeated: true, fields: fieldsToParquetSchema(type.field) }; }
    visitUnion                (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitDenseUnion           (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitSparseUnion          (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitDictionary           (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitInterval             (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitIntervalDayTime      (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitIntervalYearMonth    (type) { throw new Error(unsupportedParquetTypeErrorMessage(type.typeId));     }
    visitFixedSizeList        (_ype) { return { type: 'JSON' };                                              }
    visitMap                  (type) { return { repeated: true, fields: fieldsToParquetSchema(type.field) }; }
}

function toParquetSchema(schema) {
    return fieldsToParquetSchema(schema.fields);
}

function fieldsToParquetSchema(fields) {
    return fields.reduce((parqetSchema, field) => {
        parqetSchema[field.name] = instance.visit(field.type);
        return parqetSchema;
    }, {});
}

/** @ignore */
const instance = new GetParquetTypeVisitor();

module.exports = {
    instance,
    toParquetSchema,
    GetParquetTypeVisitor,
    fieldsToParquetSchema
}

// export type ParquetType<T extends Type> = {
//     [key: number               ]:  any ;
//     [Type.Map]:                   'JSON';
//     [Type.List]:                  'JSON';
//     [Type.FixedSizeList]:         'JSON';
//     [Type.Struct]:                'JSON';
//     [Type.Utf8]:                  'UTF8';
//     [Type.Binary]:                'BYTE_ARRAY';
//     [Type.FixedSizeBinary]:       'BYTE_ARRAY';
//     [Type.TimeMillisecond]:       'TIME_MILLIS';
//     [Type.TimeMicrosecond]:       'TIME_MICROS';
//     [Type.DateMillisecond]:       'TIMESTAMP_MILLIS';
//     [Type.TimestampMillisecond]:  'TIMESTAMP_MILLIS';
//     [Type.TimestampMicrosecond]:  'TIMESTAMP_MICROS';
//     [Type.Bool]:                  'BOOLEAN';
//     [Type.Float32]:               'FLOAT';
//     [Type.Float64]:               'DOUBLE';
//     [Type.Int8]:                  'INT_8';
//     [Type.Int16]:                 'INT_16';
//     [Type.Int32]:                 'INT_32';
//     [Type.Int64]:                 'INT_64';
//     [Type.Uint8]:                 'UINT_8';
//     [Type.Uint16]:                'UINT_16';
//     [Type.Uint32]:                'UINT_32';
//     [Type.Uint64]:                'UINT_64';

//     // unsure how these are different from int32/64?
//     // [Type.Int32]:                 'INT32';
//     // [Type.Int64]:                 'INT64';
//     // Arrow doesn't have Int96
//     // [Type.Int92]:                 'INT96';
// }[T];
