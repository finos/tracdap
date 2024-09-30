/**
 * A group of Jest tests for the util functions that handle TRAC metadata objects.
 * @category Util tests
 * @module TracMetadataConversionUtilsTests
 */

import {createUniqueObjectKey, extractValueFromTracValueObject} from "../../src/react/utils/utils_trac_metadata";
import {describe, expect, test} from '@jest/globals';
import {tracdap as trac} from "@finos/tracdap-web-api";

describe('Method for extracting a value from a primitive TRAC attribute', () => {

    const testsString: { a: trac.metadata.IValue, r: { "basicType": trac.BasicType, "subBasicType": undefined, "value": null | string } }[] = [
        {a: {type: {basicType: trac.BasicType.STRING}, stringValue: "test string"}, r: {"basicType": trac.STRING, "subBasicType": undefined, "value": "test string"}},
        {a: {type: {basicType: trac.BasicType.STRING}, stringValue: null}, r: {"basicType": trac.STRING, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.STRING}, stringValue: undefined}, r: {"basicType": trac.STRING, "subBasicType": undefined, "value": null}}
    ];

    const testsInteger: { a: trac.metadata.IValue, r: { "basicType": trac.BasicType, "subBasicType": undefined, "value": null | number } }[] = [
        {a: {type: {basicType: trac.BasicType.INTEGER}, integerValue: null}, r: {"basicType": trac.INTEGER, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.INTEGER}, integerValue: undefined}, r: {"basicType": trac.INTEGER, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.INTEGER}, integerValue: 0}, r: {"basicType": trac.INTEGER, "subBasicType": undefined, "value": 0}},
        {a: {type: {basicType: trac.BasicType.INTEGER}, integerValue: -1}, r: {"basicType": trac.INTEGER, "subBasicType": undefined, "value": -1}},
        {a: {type: {basicType: trac.BasicType.INTEGER}, integerValue: 1}, r: {"basicType": trac.INTEGER, "subBasicType": undefined, "value": 1}},
    ];

    const testsFloat: { a: trac.metadata.IValue, r: { "basicType": trac.BasicType, "subBasicType": undefined, "value": null | number } }[] = [
        {a: {type: {basicType: trac.BasicType.FLOAT}, floatValue: null}, r: {"basicType": trac.FLOAT, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.FLOAT}, floatValue: undefined}, r: {"basicType": trac.FLOAT, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.FLOAT}, floatValue: 0}, r: {"basicType": trac.FLOAT, "subBasicType": undefined, "value": 0}},
        {a: {type: {basicType: trac.BasicType.FLOAT}, floatValue: 0.0}, r: {"basicType": trac.FLOAT, "subBasicType": undefined, "value": 0.0}},
        {a: {type: {basicType: trac.BasicType.FLOAT}, floatValue: -1.7379}, r: {"basicType": trac.FLOAT, "subBasicType": undefined, "value": -1.7379}},
        {a: {type: {basicType: trac.BasicType.FLOAT}, floatValue: 1.44999999999}, r: {"basicType": trac.FLOAT, "subBasicType": undefined, "value": 1.44999999999}},
    ];

    const testsDecimal: { a: trac.metadata.IValue, r: { "basicType": trac.BasicType, "subBasicType": undefined, "value": null | string } }[] = [
        {a: {type: {basicType: trac.BasicType.DECIMAL}, decimalValue: null}, r: {"basicType": trac.DECIMAL, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.DECIMAL}, decimalValue: undefined}, r: {"basicType": trac.DECIMAL, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.DECIMAL}, decimalValue: {decimal: "0"}}, r: {"basicType": trac.DECIMAL, "subBasicType": undefined, "value": "0"}},
        {a: {type: {basicType: trac.BasicType.DECIMAL}, decimalValue: {decimal: "0.0"}}, r: {"basicType": trac.DECIMAL, "subBasicType": undefined, "value": "0.0"}},
        {a: {type: {basicType: trac.BasicType.DECIMAL}, decimalValue: {decimal: "-1.7379"}}, r: {"basicType": trac.DECIMAL, "subBasicType": undefined, "value": "-1.7379"}},
        {a: {type: {basicType: trac.BasicType.DECIMAL}, decimalValue: {decimal: "1.44999999999"}}, r: {"basicType": trac.DECIMAL, "subBasicType": undefined, "value": "1.44999999999"}},
    ];

    const testsBoolean: { a: trac.metadata.IValue, r: { "basicType": trac.BasicType, "subBasicType": undefined, "value": null | boolean } }[] = [
        {a: {type: {basicType: trac.BasicType.BOOLEAN}, booleanValue: null}, r: {"basicType": trac.BOOLEAN, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.BOOLEAN}, booleanValue: undefined}, r: {"basicType": trac.BOOLEAN, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.BOOLEAN}, booleanValue: true}, r: {"basicType": trac.BOOLEAN, "subBasicType": undefined, "value": true}},
        {a: {type: {basicType: trac.BasicType.BOOLEAN}, booleanValue: false}, r: {"basicType": trac.BOOLEAN, "subBasicType": undefined, "value": false}},
    ];

    const testsDate: { a: trac.metadata.IValue, r: { "basicType": trac.BasicType, "subBasicType": undefined, "value": null | string } }[] = [
        {a: {type: {basicType: trac.BasicType.DATE}, dateValue: null}, r: {"basicType": trac.DATE, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.DATE}, dateValue: undefined}, r: {"basicType": trac.DATE, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.DATE}, dateValue: {isoDate: null}}, r: {"basicType": trac.DATE, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.DATE}, dateValue: {isoDate: undefined}}, r: {"basicType": trac.DATE, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.DATE}, dateValue: {isoDate: "2012-10-12"}}, r: {"basicType": trac.DATE, "subBasicType": undefined, "value": "2012-10-12"}}
    ];

    const testsDatetime: { a: trac.metadata.IValue, r: { "basicType": trac.BasicType, "subBasicType": undefined, "value": null | string } }[] = [
        {a: {type: {basicType: trac.BasicType.DATETIME}, datetimeValue: null}, r: {"basicType": trac.DATETIME, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.DATETIME}, datetimeValue: undefined}, r: {"basicType": trac.DATETIME, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.DATETIME}, datetimeValue: {isoDatetime: null}}, r: {"basicType": trac.DATETIME, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.DATETIME}, datetimeValue: {isoDatetime: undefined}}, r: {"basicType": trac.DATETIME, "subBasicType": undefined, "value": null}},
        {a: {type: {basicType: trac.BasicType.DATETIME}, datetimeValue: {isoDatetime: "2012-10-12T10:10:10.000Z"}}, r: {"basicType": trac.DATETIME, "subBasicType": undefined, "value": "2012-10-12T10:10:10.000Z"}}
    ];

    const tests = [...testsString, ...testsBoolean, ...testsInteger, ...testsFloat, ...testsDecimal, ...testsDate, ...testsDatetime]

    for (let i = 0; i < tests.length; i++) {
        test(`should extract ${JSON.stringify(tests[i].r)} from object`, () => {
            expect(extractValueFromTracValueObject(tests[i].a)).toStrictEqual(tests[i].r);
        });
    }
})

describe('Method for extracting a values from a string array TRAC attribute', () => {

    const tests: { a: trac.metadata.IValue, r: { "basicType": trac.BasicType, "subBasicType": trac.BasicType, "value": [] | string[] } }[] = [
        {
            a: {type: {basicType: trac.BasicType.ARRAY, arrayType: {basicType: trac.STRING}}, arrayValue: {items: null}},
            r: {"basicType": trac.BasicType.ARRAY, "subBasicType": trac.STRING, "value": []}
        },
        {
            a: {type: {basicType: trac.BasicType.ARRAY, arrayType: {basicType: trac.STRING}}, arrayValue: {items: []}},
            r: {"basicType": trac.BasicType.ARRAY, "subBasicType": trac.STRING, "value": []}
        },
        {
            a: {type: {basicType: trac.BasicType.ARRAY, arrayType: {basicType: trac.STRING}}, arrayValue: {items: [{type: {basicType: trac.BasicType.STRING}, stringValue: "test string"}]}},
            r: {"basicType": trac.BasicType.ARRAY, "subBasicType": trac.STRING, "value": ["test string"]}
        }
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should extract ${JSON.stringify(tests[i].r)} from object`, () => {
            expect(extractValueFromTracValueObject(tests[i].a)).toStrictEqual(tests[i].r);
        });
    }
})

describe('Method for converting metadata header to a unique key', () => {

    const tests: { a: trac.metadata.ITagHeader, b: boolean, r: string }[] = [
        {a: {
            objectType: trac.ObjectType.FLOW,
            objectId: "597b589b-96fa-4e56-8066-d01fedcfcad1",
            objectVersion: 4,
            tagVersion: 5
        }, b: false, r: "FLOW-597b589b-96fa-4e56-8066-d01fedcfcad1-v4"},
        {a: {
            objectType: trac.ObjectType.DATA,
            objectId: "3327ff48-6311-4223-aa24-411f02656284",
            objectVersion: 1,
            tagVersion: 2
        }, b: true, r: "DATA-3327ff48-6311-4223-aa24-411f02656284-v1-v2"},
        {a: {
            objectType: trac.ObjectType.SCHEMA,
            objectVersion: null,
            tagVersion: null
        }, b: true, r: "SCHEMA-undefined-vnull-vnull"}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should create key '${tests[i].r}' from object ${JSON.stringify(tests[i].a)}`, () => {
            expect(createUniqueObjectKey(tests[i].a, tests[i].b)).toStrictEqual(tests[i].r);
        });
    }
})
