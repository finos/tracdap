/**
 * A group of Jest tests for the object util functions.
 * @category Util tests
 * @module ObjectUtilsTests
 */

import {describe, expect, test} from "@jest/globals";
import {objectsEqual} from "../../src/react/utils/utils_object";

describe('Method for testing if two objects are identical', () => {

    const date_1 = new Date(1900, 10, 1, 0, 0, 0, 0).toISOString()
    const date_2 = new Date(2022, 10, 2, 0, 0, 0, 0).toISOString()
    const date_3 = new Date(2022, 10, 1, 0, 0, 0, 0).toISOString()
    const date_4 = new Date(2023, 5, 10, 0, 0, 0, 0).toISOString()

    const tests: { a: Record<string, any>, b: Record<string, any>, r: boolean }[] = [
        {
            a: {x: 2, y: "name_a", z: date_1, p: true},
            b: {x: 2, y: "name_a", z: date_1, p: true},
            r: true
        },
        {
            a: {x: 2, y: "name_a", z: date_1, p: true},
            b: {x: 2, z: date_1, p: true},
            r: false
        },
        {
            a: {x: 2, y: "name_a", p: true},
            b: {x: 2, y: "name_a", z: date_1, p: true},
            r: false
        },
        {
            a: {x: 2, y: "name_a", z: {a: 1, b: "test"}, p: true},
            b: {x: 2, y: "name_a", z: {a: 1, b: "test"}, p: true},
            r: true
        },
        {
            a: {x: 2, y: "name_a", z: {a: 1, b: "test"}, p: true},
            b: {x: 2, y: "name_a", z: {a: 1, b: "error"}, p: true},
            r: false
        },
        {
            a: {},
            b: {},
            r: true
        }
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return ${tests[i].r} when testing the two objects`, () => {
            expect(objectsEqual(tests[i].a, tests[i].b)).toStrictEqual(tests[i].r);
        });
    }
})