/**
 * A group of Jest tests for the number util functions.
 * @category Util tests
 * @module NumberUtilsTests
 */

import {describe, expect, test} from "@jest/globals";
import {convertSecondsToHrMinSec, convertUtcEpochToDateTimeObject, humanReadableFileSize, roundNumberToNDecimalPlaces} from "../../src/react/utils/utils_number";

describe('Method for converting numbers to file sizes in Gb/Mb/Kb etc', () => {

    const tests: {
        a: number,
        r: string
    }[] = [
        {a: 0, r: "0 bytes"},
        {a: 512, r: "512 bytes"},
        {a: 1024, r: "1kb"},
        {a: 1024 * 1024, r: "1Mb"},
        {a: 1024 * 512, r: "512kb"},
    ]

    for (let i = 0; i < tests.length; i++) {
        test(`should return '${tests[i].r}' when given ${tests[i].a}`, () => {
            expect(humanReadableFileSize(tests[i].a)).toBe(tests[i].r);
        });
    }
})

describe('Method for rounding floats', () => {

    const tests: {
        a: number,
        b: number,
        r: number
    }[] = [
        {a: 0, b: 0, r: 0},
        {a: 0, b: 1, r: 0},
        {a: 0, b: -1, r: 0},
        {a: 1.7777777, b: 2, r: 1.78},
        {a: 9.1, b: 1, r: 9.1},
        {a: 1.255, b: 2, r: 1.26},
        {a: 1.005, b: 2, r: 1.01},
        {a: 1.05, b: 1, r: 1.1},
        {a: 1.0005, b: 3, r: 1.001},
        {a: 123.456, b: 1, r: 123.5},
        {a: 123.456, b: 2, r: 123.46},
        {a: 1.3549999999999998, b: 2, r: 1.35},
    ]

    for (let i = 0; i < tests.length; i++) {
        test(`should round ${tests[i].a} to ${tests[i].r}`, () => {
            expect(roundNumberToNDecimalPlaces(tests[i].a, tests[i].b)).toStrictEqual(tests[i].r);
        });
    }
})

describe('Method for converting a duration in seconds to a time description', () => {

    const tests: {
        a: number,
        b: boolean,
        r: string
    }[] = [
        {a: 0, b: true, r: "0 seconds"},
        {a: 0, b: false, r: "0 secs"},
        {a: 60, b: true, r: "1 minute"},
        {a: 60, b: false, r: "1 min"},
        {a: 120, b: true, r: "2 minutes"},
        {a: 120, b: false, r: "2 mins"},
        {a: 181, b: false, r: "3 mins 1 sec"},
        {a: 181.5, b: false, r: "3 mins 2 secs"},
        {a: 12000, b: true, r: "3 hours 20 minutes"},
        {a: 12000, b: false, r: "3 hrs 20 mins"},
    ]

    for (let i = 0; i < tests.length; i++) {
        test(`should convert ${tests[i].a} to ${tests[i].r}`, () => {
            expect(convertSecondsToHrMinSec(tests[i].a, tests[i].b)).toBe(tests[i].r);
        });
    }
})

describe('Method for converting a UTC epoch in milliseconds to a Javascript date', () => {

    const tests: {
        a: number,
        r: Date
    }[] = [
        {a: 0, r: new Date("1970-01-01T00:00:00.000Z")},
        {a: 60000, r: new Date("1970-01-01T00:01:00.000Z")}
    ]

    for (let i = 0; i < tests.length; i++) {
        test(`should convert ${tests[i].a} seconds to ${tests[i].r}`, () => {
            expect(convertUtcEpochToDateTimeObject(tests[i].a)).toStrictEqual(tests[i].r);
        });
    }
})

