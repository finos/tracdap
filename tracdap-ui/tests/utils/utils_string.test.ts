/**
 * A group of Jest tests for the string util functions.
 * @category Util tests
 * @module StringUtilsTests
 */

import {
    capitaliseString,
    convertKeyToText,
    isValidNumberAsStringWithCommas,
    isValidNumberAsString,
    removeNonNumericCharacters,
    getDateFormatFromString, getAllRegexMatches, convertStringValueToBoolean, removeLineBreaks, checkUrlEndsRight, isObjectKey, isObjectId, convertStringToInteger
} from "../../src/react/utils/utils_string";
import {describe, expect, test} from "@jest/globals";

describe('Method for capitalising a string', () => {

    const tests = [
        {a: "", r: ""},
        {a: "word", r: "Word"},
        {a: " word", r: " word"},
        {a: "£word", r: "£word"},
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return '${tests[i].r}' when given '${tests[i].a}'`, () => {
            expect(capitaliseString(tests[i].a)).toBe(tests[i].r);
        });
    }
})

describe('Method for converting a key to text', () => {

    const tests = [
        {a: "", b: true, r: ""},
        {a: "testString", b: true, r: "Test string"},
        {a: "testString", b: false, r: "test string"},
        {a: "testStringId", b: false, r: "test string ID"},
        {a: "test_string", b: false, r: "test string"},
        {a: "test_string_10", b: false, r: "test string 10"},
        {a: "test10", b: false, r: "test 10"},
        {a: "test_999_string", b: false, r: "test 999 string"},
        {a: "test999string", b: false, r: "test 999 string"},
        {a: "test99a9string", b: false, r: "test 99 a 9 string"}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return '${tests[i].r}' when given '${tests[i].a}'`, () => {
            expect(convertKeyToText(tests[i].a, tests[i].b)).toBe(tests[i].r);
        });
    }
})

describe('Method for checking if a string is a number (with a check on commas)', () => {

    const tests = [
        {a: "", r: false},
        {a: "0", r: true},
        {a: "-0", r: true},
        {a: "testString", r: false},
        {a: "1,000", r: true},
        {a: "9999", r: true},
        {a: "9999.9", r: true},
        {a: "9,999.9", r: true},
        {a: "99,99.9", r: false},
        {a: "123,456,789", r: true},
        {a: "-123,456,789", r: true},
        {a: "+123,456,789", r: true},
        {a: "1,23", r: false},
        {a: "1,23,456", r: false},
        {a: "1e2", r: true},
        {a: "-2e-2", r: true},
        {a: "-1.3.5", r: false},
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return ${tests[i].r} when given '${tests[i].a}'`, () => {
            expect(isValidNumberAsStringWithCommas(tests[i].a)).toBe(tests[i].r);
        });
    }
})

describe('Method for checking if a string is a number (without a check on commas)', () => {

    const tests = [
        {a: "", r: false},
        {a: "0", r: true},
        {a: "-0", r: true},
        {a: "testString", r: false},
        {a: "1,000", r: false},
        {a: "9999", r: true},
        {a: "9999.9", r: true},
        {a: "-123456789", r: true},
        {a: "+123456789", r: true},
        {a: "1e2", r: true},
        {a: "-2e-2", r: true},
        {a: "-1.3.5", r: false},
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return ${tests[i].r} when given '${tests[i].a}'`, () => {
            expect(isValidNumberAsString(tests[i].a)).toBe(tests[i].r);
        });
    }
})

describe('Method for removing non-numeric characters from a string', () => {

    const tests = [
        {a: "", r: ""},
        {a: "1234", r: "1234"},
        {a: "a123", r: "123"},
        {a: "-0", r: "-0"},
        {a: "-1.3", r: "-1.3"},
        {a: "-1.v3.5", r: "-1.3.5"},
        {a: "+1.$3.5*£", r: "1.3.5"}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return '${tests[i].r}' when given '${tests[i].a}'`, () => {
            expect(removeNonNumericCharacters(tests[i].a)).toBe(tests[i].r);
        });
    }
})

describe('Method for converting a string to an integer', () => {

    const tests = [
        {a: "", r: NaN},
        {a: "1234", r: 1234},
        {a: "a123", r: 123},
        {a: "-0", r: -0},
        {a: "-1.3", r: -1},
        {a: "-1.v3.5", r: -1},
        {a: "v1", r: 1},
        {a: "xxx", r: NaN}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return ${tests[i].r} when given '${tests[i].a}'`, () => {
            expect(convertStringToInteger(tests[i].a)).toBe(tests[i].r);
        });
    }
})

describe('Method for getting the date and datetime format from a string', () => {

    const tests = [
        {a: "31/11/2022", r: {inFormat: 'dd/MM/yyyy', type: 'DATE'}},
        {a: "30/11/2022", r: {inFormat: 'dd/MM/yyyy', type: 'DATE'}},
        {a: "2022/12/01", r: {inFormat: 'yyyy/MM/dd', type: 'DATE'}},
        {a: "2022-12-01", r: {inFormat: 'yyyy-MM-dd', type: 'DATE'}},
        {a: "31 November 2022", r: {inFormat: 'dd MMMM yyyy', type: 'DATE'}},
        {a: "30 November 2022", r: {inFormat: 'dd MMMM yyyy', type: 'DATE'}},
        {a: "30 Feb 2022", r: {inFormat: 'dd MMM yyyy', type: 'DATE'}},
        {a: "1 Feb 2022", r: {inFormat: null, type: 'STRING'}},
        {a: "01 Feb 2022", r: {inFormat: 'dd MMM yyyy', type: 'DATE'}},
        {a: "Q1 2022", r: {inFormat: 'QQQ yyyy', type: 'DATE'}},
        {a: "2010-06-15T11:01:59+02:00", r: {inFormat: 'isoDatetime', type: 'DATETIME'}},
        {a: "2010-06-15T11:01:59Z", r: {inFormat: 'isoDatetime', type: 'DATETIME'}},
        {a: "1997-07-16T19:20:30.123+01:00", r: {inFormat: 'isoDatetime', type: 'DATETIME'}},
        {a: "2010-06-15T11:01:59.123Z", r: {inFormat: 'isoDatetime', type: 'DATETIME'}},
        {a: "", r: {inFormat: null, type: 'STRING'}},
        {a: "xxx", r: {inFormat: null, type: 'STRING'}}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return ${JSON.stringify(tests[i].r)} when given '${tests[i].a}'`, () => {
            expect(getDateFormatFromString(tests[i].a)).toStrictEqual(tests[i].r);
        });
    }
})

describe('Method for getting all regex matches from a string with the option to set groups', () => {

    const tests = [
        {a: "VARIABLE_1", b: /^(\w+)$/g, c: undefined, r: ['VARIABLE_1']},
        {a: "VARIABLE_1", b: /^(\w+)$/g, c: 1, r: [['VARIABLE_1']]},
        {
            a: "sum(VARIABLE_1) as SUM_VARIABLE_1",
            b: /^(\w+)\s*\(\s*(\w+)\s*\)\s+as\s+(\w+)$/g,
            c: 3,
            r: [['sum', 'VARIABLE_1', 'SUM_VARIABLE_1']]
        },
        {
            a: "sum(VARIABLE_1) as SUM_VARIABLE_1, count(VARIABLE_2) as COUNT_VARIABLE_2",
            b: /(\w+)\s*\(\s*(\w+)\s*\)\s+as\s+(\w+)/g,
            c: 3,
            r: [['sum', 'VARIABLE_1', 'SUM_VARIABLE_1'], ['count', 'VARIABLE_2', 'COUNT_VARIABLE_2']]
        },
        {a: "abc", b: /^(def)$/g, c: undefined, r: []},
        {a: "abc", b: /^(def)$/g, c: 1, r: []},
        {a: "abc", b: /^(de)(fg)$/g, c: 2, r: []}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return ${JSON.stringify(tests[i].r)} when given '${tests[i].a}'`, () => {
            if (tests[i].c === undefined) {
                expect(getAllRegexMatches(tests[i].a, tests[i].b)).toStrictEqual(tests[i].r);
            } else {
                expect(getAllRegexMatches(tests[i].a, tests[i].b, tests[i].c || 1)).toStrictEqual(tests[i].r);
            }
        });
    }
})

describe('Method for converting a string to a boolean', () => {

    const tests = [
        {a: "", r: null},
        {a: "true", r: true},
        {a: "FALSE", r: false},
        {a: "xxx", r: null},
        {a: "", r: null},
        {a: null, r: null}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return '${tests[i].r}' when given '${tests[i].a}'`, () => {
            expect(convertStringValueToBoolean(tests[i].a)).toBe(tests[i].r);
        });
    }
})

describe('Method for removing line breaks from a string', () => {

    const tests = [
        {a: "", r: ""},
        {a: "abc", r: "abc"},
        {a: "abc\ndef", r: "abc def"},
        {a: "abc\rdef", r: "abc def"},
        {a: "abc\r\ndef", r: "abc def"},
        {a: "abc\r\n", r: "abc "}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return '${tests[i].r}' when given '${tests[i].a}'`, () => {
            expect(removeLineBreaks(tests[i].a)).toBe(tests[i].r);
        });
    }
})


describe('Method for ensuring that a string ends in a slash', () => {

    const tests = [
        {a: "", r: "/"},
        {a: "abc", r: "abc/"},
        {a: "abc/", r: "abc/"}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return '${tests[i].r}' when given '${tests[i].a}'`, () => {
            expect(checkUrlEndsRight(tests[i].a)).toBe(tests[i].r);
        });
    }
})

describe('Method for checking if a string is a valid object key', () => {

    const tests: { a: any, r: boolean }[] = [
        {a: "FLOW-597b589b-96fa-4e56-8066-d01fedcfcad1-v4", r: true},
        {a: "597b589b-96fa-4e56-8066-d01fedcfcad1-v4", r: false},
        {a: "DATA-3327ff48-6311-4223-aa24-411f02656284-v1-v2", r: false},
        {a: "SCHEMA-undefined-vnull-vnull", r: false},
        {a: null, r: false},
        {a: {}, r: false},
        {a: 999, r: false}

    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should check key '${tests[i].a}' as ${tests[i].a}`, () => {
            expect(isObjectKey(tests[i].a)).toBe(tests[i].r);
        });
    }
})

describe('Method for checking if a string is a valid object ID', () => {

    const tests: { a: any, r: boolean }[] = [
        {a: "FLOW-597b589b-96fa-4e56-8066-d01fedcfcad1-v4", r: false},
        {a: "597b589b-96fa-4e56-8066-d01fedcfcad1", r: true},
        {a: "597b589b-96fa", r: false},
        {a: null, r: false},
        {a: {}, r: false},
        {a: 999, r: false}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should check ID '${tests[i].a}' as ${tests[i].a}`, () => {
            expect(isObjectId(tests[i].a)).toBe(tests[i].r);
        });
    }
})
