/**
 * A group of Jest tests for the general util functions.
 * @category Util tests
 * @module GeneralUtilsTests
 */

import {calculateModelEntryPointAndPath} from "../../src/react/utils/utils_general";
import {describe, expect, test} from '@jest/globals';

describe('Method for calculating a model endpoint and path', () => {

    const tests: {
        a: { packages: string[], path: string[] },
        b: string,
        c?: string
        r: { entryPoint: null | string, path: string }
    }[] = [
        {a: {packages: ["a", "b", "c"], path: ["d", "e", "f"]}, b: "filename", c: "class", r: {entryPoint: "a.b.c.filename.class", path: "d/e/f"}},
        {a: {packages: ["a", "b", "c"], path: ["d", "e", "f"]}, b: "filename", c: undefined, r: {entryPoint: null, path: "d/e/f"}},
        {a: {packages: ["a", "b", "c"], path: []}, b: "filename", c: undefined, r: {entryPoint: null, path: "."}}
    ]

    for (let i = 0; i < tests.length; i++) {
        test(`should calculate entryPoint as '${tests[i].r.entryPoint}' and path as ${tests[i].r.path}`, () => {
            expect(calculateModelEntryPointAndPath(tests[i].a, tests[i].b, tests[i].c)).toStrictEqual(tests[i].r);
        });
    }
})