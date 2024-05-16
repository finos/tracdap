/**
 * A group of Jest tests for the formatting util functions.
 * @category Util tests
 * @module FormatUtilsTests
 */

import {
    applyNumberFormat,
    assignDefaultNumberFormat,
    convertDateObjectToFormatCode,
    convertIsoDateStringToFormatCode,
    convertNumberFormatCodeToArray,
    convertQuarterFormatToHalfYear,
    isValidNumberFormatCode,
    setAnyFormat
} from "../../src/react/utils/utils_formats";
import {describe, expect, test} from "@jest/globals";
import {DateFormat, DatetimeFormat, NumberFormatAsArray} from "../../src/types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {isDateFormat, isDateObject, isTracDateOrDatetime} from "../../src/react/utils/utils_trac_type_chckers";

describe('Method for checking if a string is a valid numeric format code', () => {

    const tests = [
        {a: null, r: false},
        {a: undefined, r: false}
    ];

    for (let i = 0; i < tests.length; i++) {
        test('should return false for missing values', () => {
            expect(isValidNumberFormatCode(tests[i].a)).toBe(tests[i].r);
        });
    }
})

describe('Method for checking is a string is a valid numeric format code', () => {

    const tests: { a: string, r: boolean }[] = [
        {a: "", r: false},
        {a: "|||", r: false},
        {a: "||||", r: true},
        {a: "|||||", r: true},
        {a: "||||||", r: false},
        {a: ",|.|0|$|%", r: true},
        {a: ",|.|0|$|1", r: true},
        {a: ",|.||$|1", r: true},
        {a: ",|.|0|$|", r: true},
        {a: ",|.|x|$|1", r: false},
        {a: ",|.|2|$|x", r: true},
        {a: ",|.|3|$|1|", r: true},
        {a: ",|.|2|$|x|0", r: true},
        {a: ",|.|2|$|x|y", r: false}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should verify string '${tests[i].a}' as ${tests[i].r}`, () => {
            expect(isValidNumberFormatCode(tests[i].a)).toBe(tests[i].r);
        });
    }
})

describe('Method for converting a string format code into the array equivalent for number TRAC basic types', () => {

    const tests: {
        a: null | undefined | string,
        b: undefined | trac.BasicType.INTEGER | trac.BasicType.DECIMAL | trac.BasicType.FLOAT,
        r: NumberFormatAsArray
    }[] = [
        {a: null, b: trac.INTEGER, r: convertNumberFormatCodeToArray(assignDefaultNumberFormat(trac.INTEGER), trac.INTEGER)},
        {a: undefined, b: trac.FLOAT, r: convertNumberFormatCodeToArray(assignDefaultNumberFormat(trac.FLOAT), trac.FLOAT)},
        {a: "", b: trac.DECIMAL, r: convertNumberFormatCodeToArray(assignDefaultNumberFormat(trac.DECIMAL), trac.DECIMAL)},
        {a: "|||", b: undefined, r: convertNumberFormatCodeToArray(assignDefaultNumberFormat(undefined), undefined)},
        {a: ",|.|2|x|y|100", b: undefined, r: [",", ".", 2, "x", "y", 100]},
        {a: ",|.|2|x|y|100", b: trac.FLOAT, r: [",", ".", 2, "x", "y", 100]},
        {a: ",|.|2|x|y|", b: trac.DECIMAL, r: [",", ".", 2, "x", "y", 1]},
        {a: ",|.|2|x|y|100", b: trac.FLOAT, r: [",", ".", 2, "x", "y", 100]},
        {a: ",|.|2|x|y|", b: trac.INTEGER, r: [",", ".", 2, "x", "y", 1]},
        {a: ",|.||x|y|", b: trac.FLOAT, r: [",", ".", null, "x", "y", 1]}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should convert string '${tests[i].a}' to ${JSON.stringify(tests[i].r)}`, () => {
            expect(convertNumberFormatCodeToArray(tests[i].a, tests[i].b)).toStrictEqual(tests[i].r);
        });
    }
})

describe('Method for converting a Javascript date to a formatted string', () => {

    const date = new Date(2022, 11, 9, 20, 14, 23, 300)
    const tests: {
        a: Date,
        b: DateFormat | DatetimeFormat,
        r: string
    }[] = [
        {a: date, b: "DATETIME", r: "9th Dec 2022 20:14:23"},
        {a: date, b: "DAY", r: "9th December 2022"},
        {a: date, b: "WEEK", r: "Week 49 2022"},
        {a: date, b: "MONTH", r: "Dec 2022"},
        {a: date, b: "QUARTER", r: "Q4 2022"},
        {a: date, b: "HALF_YEAR", r: "H2 2022"},
        {a: date, b: "YEAR", r: "2022"},
        {a: date, b: "ISO", r: "2022-12-09"},
        {a: date, b: "TIME", r: "20:14"},
        {a: date, b: "FILENAME", r: "2022_12_09_201423"},
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should convert date using format '${tests[i].b}' to '${tests[i].r}`, () => {
            expect(convertDateObjectToFormatCode(tests[i].a, tests[i].b)).toBe(tests[i].r);
        });
    }
})

describe('Method for converting a date ISO string to a formatted string', () => {

    const tests: {
        a: string,
        b: DateFormat | DatetimeFormat,
        r: string | typeof NaN
    }[] = [
        {a: "2022-11-09T20:14:23.300Z", b: "DATETIME", r: "9th Nov 2022 20:14:23"},
        {a: "1997-07-16T19:20:30.123+02:00", b: "DATETIME", r: "16th Jul 1997 18:20:30"},
        {a: "2022-11-09T20:14:23.300Z", b: "DAY", r: "9th November 2022"},
        {a: "2022-11-09T20:14:23.300Z", b: "WEEK", r: "Week 45 2022"},
        {a: "2022-11-09T20:14:23.300Z", b: "MONTH", r: "Nov 2022"},
        {a: "2022-11-09T20:14:23.300Z", b: "QUARTER", r: "Q4 2022"},
        {a: "2022-11-09T20:14:23.300Z", b: "HALF_YEAR", r: "H2 2022"},
        {a: "2022-11-09T20:14:23.300Z", b: "YEAR", r: "2022"},
        {a: "2022-11-09T20:14:23.300Z", b: "ISO", r: "2022-11-09"},
        {a: "2022-11-09T08:20:23.300Z", b: "TIME", r: "08:20"},
        {a: "2022-11-09T20:14:23.300Z", b: "FILENAME", r: "2022_11_09_201423"},
        {a: "2022-11-09", b: "DATETIME", r: "9th Nov 2022 00:00:00"},
        {a: "1997-07-16", b: "DATETIME", r: "16th Jul 1997 00:00:00"},
        {a: "2022-11-09", b: "DAY", r: "9th November 2022"},
        {a: "2022-11-09", b: "WEEK", r: "Week 45 2022"},
        {a: "2022-11-09", b: "MONTH", r: "Nov 2022"},
        {a: "2022-11-09", b: "QUARTER", r: "Q4 2022"},
        {a: "2022-11-09", b: "HALF_YEAR", r: "H2 2022"},
        {a: "2022-11-09", b: "YEAR", r: "2022"},
        {a: "2022-11-09", b: "ISO", r: "2022-11-09"},
        {a: "2022-11-09", b: "TIME", r: "00:00"},
        {a: "2022-11-09", b: "FILENAME", r: "2022_11_09_000000"}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should convert date using format '${tests[i].b}' to '${tests[i].r}`, () => {
            expect(convertIsoDateStringToFormatCode(tests[i].a, tests[i].b)).toBe(tests[i].r);
        });
    }
})

describe('Method for converting a date string for a quarterly date to a half-year date', () => {

    const tests: {
        a: string,
        r: string
    }[] = [
        {a: "Q1 2023", r: "H1 2023"},
        {a: "Q2 2023", r: "H1 2023"},
        {a: "Q3 2023", r: "H2 2023"},
        {a: "Q4 2023", r: "H2 2023"},
        {a: " Q4  2023", r: " Q4  2023"},
        {a: " Q4 2023", r: " H2 2023"},
        {a: "xxx", r: "xxx"}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should convert quarter date string '${tests[i].a}' to '${tests[i].r}`, () => {
            expect(convertQuarterFormatToHalfYear(tests[i].a)).toBe(tests[i].r);
        });
    }
})

describe('Method for formatting a number', () => {

    const tests: {
        a: number,
        b: string | null | undefined,
        c: boolean,
        r: string
    }[] = [
        {a: 0, b: "|||||", c: true, r: "0"},
        {a: 0, b: ",|.|0|$|m", c: true, r: "$0m"},
        {a: -100, b: ",|.|0|$|m", c: true, r: "-$100m"},
        {a: 123456, b: ",|.|0||", c: true, r: "123,456"},
        {a: 1234567, b: ",|.|0||", c: true, r: "1,234,567"},
        {a: -100, b: ",|.|3|$|m", c: true, r: "-$100.000m"},
        {a: -100.1249999, b: ",|.|2|$|m", c: true, r: "-$100.12m"},
        {a: 0.01249999, b: ",|.|2||%|100", c: true, r: "1.25%"},
        {a: -101.5249999, b: ",|.|2|$|m", c: false, r: "-101.52"},
        {a: 0.01249999, b: ",|.|2||%|100", c: false, r: "1.25"},
        // These are using the default format for a float since the format is invalid
        {a: 0.02249999, b: null, c: true, r: "0.02"},
        {a: 0.03249999, b: undefined, c: true, r: "0.03"}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should format the number ${tests[i].a} to '${tests[i].r}`, () => {
            expect(applyNumberFormat(tests[i].a, tests[i].b, tests[i].c)).toBe(tests[i].r);
        });
    }
})

describe('Method for formatting a number or date/datetime, other types are passed through', () => {

    const tests: {
        a: trac.BasicType,
        b: null | string | DatetimeFormat | DateFormat,
        c: null | boolean | string | number | Date,
        r: null | string
    }[] = [
        {a: trac.INTEGER, b: "|||||", c: 0, r: "0"},
        {a: trac.FLOAT, b: ",|.|0|$|m", c: 0, r: "$0m"},
        {a: trac.DECIMAL, b: ",|.|0|$|m", c: -100, r: "-$100m"},
        {a: trac.INTEGER, b: ",|.|0||", c: 123456, r: "123,456"},
        {a: trac.FLOAT, b: ",|.|0||", c: 1234567, r: "1,234,567"},
        {a: trac.INTEGER, b: ",|.|3|$|m", c: -100, r: "-$100.000m"},
        {a: trac.FLOAT, b: ",|.|2|$|m", c: -100.1249999, r: "-$100.12m"},
        {a: trac.INTEGER, b: ",|.|2||%|100", c: 0.01249999, r: "1.25%"},
        {a: trac.INTEGER, b: ",|.|2||%|100", c: null, r: null},
        {a: trac.DATE, b: "DATETIME", c: "2022-11-09", r: "9th Nov 2022 00:00:00"},
        {a: trac.DATE, b: "DATETIME", c: "1997-07-16", r: "16th Jul 1997 00:00:00"},
        {a: trac.DATE, b: "DAY", c: "2022-11-09", r: "9th November 2022"},
        {a: trac.DATE, b: "WEEK", c: "2022-11-09", r: "Week 45 2022"},
        {a: trac.DATE, b: "MONTH", c: "2022-11-09", r: "Nov 2022"},
        {a: trac.DATE, b: "QUARTER", c: "2022-11-09", r: "Q4 2022"},
        {a: trac.DATE, b: "HALF_YEAR", c: "2022-11-09", r: "H2 2022"},
        {a: trac.DATE, b: "YEAR", c: "2022-11-09", r: "2022"},
        {a: trac.DATE, b: "ISO", c: "2022-11-09", r: "2022-11-09"},
        {a: trac.DATE, b: "TIME", c: "2022-11-09", r: "00:00"},
        {a: trac.DATE, b: "FILENAME", c: "2022-11-09", r: "2022_11_09_000000"},
        {a: trac.DATETIME, b: "DATETIME", c: "2022-11-09T20:14:23.300Z", r: "9th Nov 2022 20:14:23"},
        {a: trac.DATETIME, b: "DATETIME", c: "1997-07-16T19:20:30.123+02:00", r: "16th Jul 1997 18:20:30"},
        {a: trac.DATETIME, b: "DAY", c: "2022-11-09T20:14:23.300Z", r: "9th November 2022"},
        {a: trac.DATETIME, b: "WEEK", c: "2022-11-09T20:14:23.300Z", r: "Week 45 2022"},
        {a: trac.DATETIME, b: "MONTH", c: "2022-11-09T20:14:23.300Z", r: "Nov 2022"},
        {a: trac.DATETIME, b: "QUARTER", c: "2022-11-09T20:14:23.300Z", r: "Q4 2022"},
        {a: trac.DATETIME, b: "HALF_YEAR", c: "2022-11-09T20:14:23.300Z", r: "H2 2022"},
        {a: trac.DATETIME, b: "YEAR", c: "2022-11-09T20:14:23.300Z", r: "2022"},
        {a: trac.DATETIME, b: "ISO", c: "2022-11-09T20:14:23.300Z", r: "2022-11-09"},
        {a: trac.DATETIME, b: "TIME", c: "2022-11-09T08:20:23.300Z", r: "08:20"},
        {a: trac.DATETIME, b: "FILENAME", c: "2022-11-09T20:14:23.300Z", r: "2022_11_09_201423"},
        {a: trac.STRING, b: null, c: "xxx", r: "xxx"},
        {a: trac.STRING, b: null, c: "", r: ""},
        {a: trac.STRING, b: null, c: null, r: null},
        {a: trac.BOOLEAN, b: null, c: true, r: "True"},
        {a: trac.BOOLEAN, b: null, c: false, r: "False"},
        {a: trac.BOOLEAN, b: null, c: null, r: null},
        {a: trac.DATE, b: null, c: "2022-11-09", r: "2022-11-09"},
        {a: trac.DATETIME, b: null, c: "2022-11-09", r: "2022-11-09"},
        {a: trac.FLOAT, b: null, c: -101.5249999, r: "-101.5249999"},
        {a: trac.INTEGER, b: null, c: 0.01249999, r: "0.01249999"},
        {a: trac.DATE, b: "DAY", c: new Date(2023, 1,10,4,45,32,5), r: "10th February 2023"},
        {a: trac.DATETIME, b: "DATETIME", c: new Date(2023, 1,10,4,45,32,5), r: "10th Feb 2023 04:45:32"},
        {a: trac.DATE, b: "DATETIME", c: "101", r: "101"}
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should format the value ${tests[i].a} to '${tests[i].r}`, () => {
            expect(setAnyFormat(tests[i].a, tests[i].b, tests[i].c)).toBe(tests[i].r);
        });
    }
})