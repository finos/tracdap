/**
 * A group of Jest tests for the array util functions.
 * @category Util tests
 * @module ArrayUtilsTests
 */

import {
    arraysOfObjectsEqual,
    arraysOfObjectsEqualByKey,
    arraysOfPrimitiveValuesEqual,
    arraysOfPrimitiveValuesEqualInOrder,
    convertArrayToOptions,
    countItemsInArray,
    duplicatesInArray,
    getMinAndMaxValueFromArrayOfObjects,
    getUniqueObjectIndicesFromArray,
    makeArrayOfObjectsUnique,
    makeArrayOfObjectsUniqueByProperty,
    sortArrayBy
} from "../../src/react/utils/utils_arrays";
import type {DateFormat, DatetimeFormat, Option} from "../../src/types/types_general";
import {describe, expect, test} from "@jest/globals";
import {tracdap as trac} from "@finos/tracdap-web-api";

describe('Method for checking if two arrays of objects are equal', () => {

    const today = new Date()

    const tests = [
        {
            a: [
                {x: 1, y: "test", z: today, p: {q: 999, r: true}},
                {x: false, y: "start", z: null, p: {q: 999, r: true}}
            ],
            b: [
                {x: 1, y: "test", z: today, p: {q: 999, r: true}},
                {x: false, y: "start", z: null, p: {q: 999, r: true}}
            ],
            r: true
        },
        {
            a: [
                {x: 1, y: "test", z: today, p: {q: 999, r: true}},
                {x: 1, y: "test", z: today, p: {q: 999, r: true}}
            ],
            b: [
                {x: 1, y: "test", z: today, p: {q: 999, r: true}},
                {x: 1, y: "test", z: today, p: {q: 998, r: true}}
            ],
            r: false
        },
        {
            a: [
                {x: 1, y: "test", z: today, p: {q: 999, r: true}},
                {x: 1, y: "test", z: today, p: {q: 999, r: true}}
            ],
            b: [
                {x: 1, y: "test", z: today},
                {x: 1, y: "test", z: today, p: {q: 998, r: true}}
            ],
            r: false
        }
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return ${tests[i].r} for array comparison`, () => {
            expect(arraysOfObjectsEqual(tests[i].a, tests[i].b)).toStrictEqual(tests[i].r);
        });
    }
})

describe('Method for checking if two arrays of objects are equal by a key', () => {

    const today = new Date()

    const tests = [
        {
            a: [
                {x: 1, y: "name1", z: today, p: {q: 999, r: true}},
                {x: 2, y: "name2", z: today, p: {q: 100, r: false}}
            ],
            b: [
                {x: 2, y: "name2", z: today, p: {q: 100, r: false}},
                {x: 1, y: "name1", z: today, p: {q: 999, r: true}}
            ],
            c: "y",
            r: true
        },
        {
            a: [
                {x: 1, y: "name1", z: today},
                {x: 2, y: "name2", z: today, p: {q: 100, r: false}}
            ],
            b: [
                {x: 2, y: "name2", z: today, p: {q: 100, r: false}},
                {x: 1, y: "name1", z: today, p: {q: 999, r: true}}
            ],
            c: "y",
            r: false
        },
        {
            a: [
                {x: 1, y: "name1", z: today, p: {q: 999, r: true}},
                {x: 2, y: "name2", z: today, p: {q: 100, r: false, s: false}}
            ],
            b: [
                {x: 2, y: "name2", z: today, p: {q: 100, r: false}},
                {x: 1, y: "name1", z: today, p: {q: 999, r: true}}
            ],
            c: "y",
            r: false
        }
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return ${tests[i].r} for array comparison, using '${tests[i].c}' as the lookup key`, () => {
            expect(arraysOfObjectsEqualByKey(tests[i].a, tests[i].b, tests[i].c)).toStrictEqual(tests[i].r);
        });
    }
})

describe('Method for checking if two arrays of primitive values are equal without order mattering', () => {

    const tests = [
        {
            a: ["x", "y", "z"],
            b: ["x", "y", "z"],
            r: true
        },
        {
            a: [3, 2, 1, null],
            b: [1, null, 2, 3],
            r: true
        },
        {
            a: [3, 2, 1],
            b: [1, null, 2, 3],
            r: false
        },
        {
            a: [3, undefined, 2, 1],
            b: [1, null, 2, 3],
            r: false
        },
        {
            a: [3, 2, "x", 1, undefined, false],
            b: [1, false, undefined, 2, 3, "x"],
            r: true
        },
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return ${tests[i].r} for array comparison`, () => {
            expect(arraysOfPrimitiveValuesEqual(tests[i].a, tests[i].b)).toBe(tests[i].r);
        });
    }
})

describe('Method for checking if two arrays of primitive values are equal with order mattering', () => {

    const tests = [
        {
            a: ["x", "y", "z"],
            b: ["x", "y", "z"],
            r: true
        },
        {
            a: [3, 2, 1, null],
            b: [1, null, 2, 3],
            r: false
        },
        {
            a: [3, 2, 1],
            b: [1, null, 2, 3],
            r: false
        },
        {
            a: [3, undefined, 2, 1],
            b: [1, null, 2, 3],
            r: false
        },
        {
            a: [3, 2, "x", 1, undefined, false],
            b: [1, false, undefined, 2, 3, "x"],
            r: false
        },
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return ${tests[i].r} for array comparison`, () => {
            expect(arraysOfPrimitiveValuesEqualInOrder(tests[i].a, tests[i].b)).toBe(tests[i].r);
        });
    }
})

describe('Method for counting the number of items in a string array', () => {

    const tests = [
        {
            a: ["x", "y", "z"],
            b: false,
            r: {X: 1, Y: 1, Z: 1}
        },
        {
            a: ["x", "y", "z", "x", "x", "y"],
            b: false,
            r: {X: 3, Y: 2, Z: 1}
        },
        {
            a: ["x", "y", "z", "x", "X", "y"],
            b: true,
            r: {x: 2, y: 2, z: 1, X: 1}
        },
        {
            a: ["x", "y", "z", "x", "X", "y"],
            b: false,
            r: {X: 3, Y: 2, Z: 1}
        },
        {
            a: [],
            b: false,
            r: {}
        },
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return ${JSON.stringify(tests[i].r)} for array ${JSON.stringify(tests[i].a)}`, () => {
            expect(countItemsInArray(tests[i].a, tests[i].b)).toStrictEqual(tests[i].r);
        });
    }
})

describe('Method for counting the duplicate items in a string array', () => {

    const tests = [
        {
            a: ["x", "y", "z"],
            b: false,
            r: []
        },
        {
            a: ["x", "y", "z", "x", "x", "y"],
            b: false,
            r: ["X", "Y"]
        },
        {
            a: ["a", "y", "z", "a", "A", "y", "A"],
            b: true,
            r: ["a", "y", "A"]
        },
        {
            a: ["a", "y", "z", "a", "A", "y"],
            b: false,
            r: ["A", "Y"]
        },
        {
            a: [],
            b: false,
            r: []
        },
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return ${JSON.stringify(tests[i].r)} for array ${JSON.stringify(tests[i].a)}`, () => {
            expect(duplicatesInArray(tests[i].a, tests[i].b)).toStrictEqual(tests[i].r);
        });
    }
})

describe('Method for making an array of objects unique', () => {

    const today = new Date()

    const tests = [
        {
            a: [
                {x: 1, y: "test", z: today, p: {q: 999, r: true}},
                {x: 1, y: "test", z: today, p: {q: 999, r: true}}
            ],
            r: [
                {x: 1, y: "test", z: today, p: {q: 999, r: true}}
            ]
        },
        {
            a: [
                {x: 1, y: "test", z: today, p: {q: 999, r: true}},
                {x: 1, y: "test", z: today, p: {q: 999, r: true}},
                {x: 2, y: "start", z: today, p: {q: 999, r: true}}
            ],
            r: [
                {x: 1, y: "test", z: today, p: {q: 999, r: true}},
                {x: 2, y: "start", z: today, p: {q: 999, r: true}}
            ]
        },
        {
            a: [],
            r: []
        },
        {
            a: [{x: 1, y: "test", z: today, p: {q: 999, r: true}},],
            r: [{x: 1, y: "test", z: today, p: {q: 999, r: true}},]
        }
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return ${tests[i].r} for array ${JSON.stringify(tests[i].a)}`, () => {
            expect(makeArrayOfObjectsUnique(tests[i].a)).toStrictEqual(tests[i].r);
        });
    }
})

describe('Method for making an array of objects unique by a property', () => {

    const today = new Date()

    const tests = [
        {
            a: [
                {x: 1, y: "test1", z: today, p: {q: 999, r: true}},
                {x: 1, y: "test1", z: today, p: {q: 999, r: true}}
            ],
            b: "y",
            r: [
                {x: 1, y: "test1", z: today, p: {q: 999, r: true}},
            ]
        },
        {
            a: [],
            b: "y",
            r: []

        },
        {
            a: [{x: 1, y: "test1", z: today, p: {q: 999, r: true}},
                {x: 1, y: "test2", z: today, p: {q: 999, r: true}}
            ],
            b: "xxx",
            r: []
        },
        {
            a: [
                {x: 1, y: "test1", z: today, p: {q: 999, r: true}},
                {x: 1, y: "test2", z: today, p: {q: 999, r: true}}
            ],
            b: "y",
            r: [
                {x: 1, y: "test1", z: today, p: {q: 999, r: true}},
                {x: 1, y: "test2", z: today, p: {q: 999, r: true}}
            ]
        }
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return ${JSON.stringify(tests[i].r)} for array ${JSON.stringify(tests[i].a)}`, () => {
            expect(makeArrayOfObjectsUniqueByProperty(tests[i].a, tests[i].b)).toStrictEqual(tests[i].r);
        });
    }
})

describe('Method for calculating the minimum and maximum of a property in an array of objects', () => {

    const date_1 = new Date(1900, 10, 1, 0, 0, 0, 0).toISOString()
    const date_2 = new Date(2022, 10, 1, 0, 0, 0, 0).toISOString()
    const date_3 = new Date(2022, 10, 2, 0, 0, 0, 0).toISOString()

    const tests = [
        {
            a: [
                {x: 1},
                {x: 3},
                {x: 5},
                {x: 5},
                {x: 4}
            ],
            b: "x",
            r: {minimum: 1, maximum: 5}
        },
        {
            a: [
                {y: "john"},
                {y: "george"},
                {y: "ringo"},
                {y: "paul"}
            ],
            b: "y",
            r: {minimum: "george", maximum: "ringo"}
        },
        {
            a: [
                {z: date_1},
                {z: date_2},
                {z: date_2},
                {z: date_3}
            ],
            b: "z",
            r: {minimum: date_1, maximum: date_3}
        },
        {
            a: [
                {y: "john"},
                {y: null},
                {y: "ringo"},
            ],
            b: "y",
            r: {minimum: "john", maximum: "ringo"}
        },
        {
            a: [
                {p: ["a", "b", "c"]},
                {p: ["d", "e"]},
                {p: ["f"]},
                {p: ["g", "h", "i"]}
            ],
            b: "p",
            r: {minimum: undefined, maximum: undefined}
        },
        {
            a: [
                {y: false},
                {y: true},
                {y: "ringo"},
                {y: null}
            ],
            b: "y",
            r: {minimum: false, maximum: true}
        }
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return ${JSON.stringify(tests[i].r)} for array ${JSON.stringify(tests[i].a)}`, () => {
            expect(getMinAndMaxValueFromArrayOfObjects(tests[i].a, tests[i].b)).toStrictEqual(tests[i].r);
        });
    }
})

describe('Method for converting an array to a set of options', () => {

    const testsNumber: { a: (null | number)[], b: boolean, c: trac.BasicType, d?: string, r: Option<null | number>[] }[] = [
        {
            a: [1, 3, 5, 5, 4, null],
            b: true,
            c: trac.INTEGER,
            d: undefined,
            r: [
                {value: 1, label: "1"},
                {value: 3, label: "3"},
                {value: 5, label: "5"},
                {value: 4, label: "4"},
                {value: null, label: "None"}
            ]
        },
        {
            a: [1, 4.8, 1.4499999999],
            b: false,
            c: trac.FLOAT,
            d: ",|.|2||%|1",
            r: [
                {value: 1, label: "1.00%"},
                {value: 4.8, label: "4.80%"},
                {value: 1.4499999999, label: "1.45%"}
            ]
        }
    ]

    const testsString: { a: (null | string)[], b: boolean, c: trac.BasicType, d?: string, r: Option<null | string>[] }[] = [
        {
            a: ["john", "george"],
            b: false,
            c: trac.STRING,
            d: undefined,
            r: [
                {value: "john", label: "john"},
                {value: "george", label: "george"}
            ]
        },
        {
            a: ["ringo", "paul"],
            b: true,
            c: trac.STRING,
            d: undefined,
            r: [
                {value: "ringo", label: "Ringo"},
                {value: "paul", label: "Paul"}
            ]
        }
    ]

    const testsDate: { a: (null | string)[], b: boolean, c: trac.BasicType, d?: DatetimeFormat | DateFormat, r: Option<null | string>[] }[] = [
        {
            a: ["2022-10-05", "2020-12-05", "1990-05-30"],
            b: false,
            c: trac.DATE,
            d: "MONTH",
            r: [
                {value: "2022-10-05", label: "Oct 2022"},
                {value: "2020-12-05", label: "Dec 2020"},
                {value: "1990-05-30", label: "May 1990"}
            ]
        }
    ]

    const testsBoolean: { a: (null | boolean)[], b: boolean, c: trac.BasicType, d?: DatetimeFormat | DateFormat, r: Option<null | boolean>[] }[] = [
        {
            a: [true, false],
            b: false,
            c: trac.BOOLEAN,
            d: undefined,
            r: [
                {value: true, label: "true"},
                {value: false, label: "false"}
            ]
        }
    ]

    let testsGroup1 = [...testsNumber]

    for (let i = 0; i < testsGroup1.length; i++) {
        test(`should return ${JSON.stringify(testsGroup1[i].r)} for array ${JSON.stringify(testsGroup1[i].a)}`, () => {
            expect(convertArrayToOptions(testsGroup1[i].a, testsGroup1[i].b, testsGroup1[i].c, testsGroup1[i].d)).toStrictEqual(testsGroup1[i].r);
        });
    }

    let testsGroup2 = [...testsBoolean]

    for (let i = 0; i < testsGroup2.length; i++) {
        test(`should return ${JSON.stringify(testsGroup2[i].r)} for array ${JSON.stringify(testsGroup2[i].a)}`, () => {
            expect(convertArrayToOptions(testsGroup2[i].a, testsGroup2[i].b, testsGroup2[i].c, testsGroup2[i].d)).toStrictEqual(testsGroup2[i].r);
        });
    }

    let testsGroup3 = [...testsString, ...testsDate]

    for (let i = 0; i < testsGroup3.length; i++) {
        test(`should return ${JSON.stringify(testsGroup3[i].r)} for array ${JSON.stringify(testsGroup3[i].a)}`, () => {
            expect(convertArrayToOptions(testsGroup3[i].a, testsGroup3[i].b, testsGroup3[i].c, testsGroup3[i].d)).toStrictEqual(testsGroup3[i].r);
        });
    }
})


describe('Method for sorting an array of objects by a key', () => {

    const date_1 = new Date(1900, 10, 1, 0, 0, 0, 0).toISOString()
    const date_2 = new Date(2022, 10, 2, 0, 0, 0, 0).toISOString()
    const date_3 = new Date(2022, 10, 1, 0, 0, 0, 0).toISOString()
    const date_4 = new Date(2023, 5, 10, 0, 0, 0, 0).toISOString()

    const tests: { a: { x: number, y: string, z: string, p: boolean }[], b: "x" | "y" | "z" | "p", r: { x: number, y: string, z: string, p: boolean }[] }[] = [
        {
            a: [
                {x: 2, y: "name_a", z: date_1, p: true},
                {x: 1, y: "name_d", z: date_2, p: false},
                {x: 5, y: "name_c", z: date_3, p: true},
                {x: 4, y: "name_b", z: date_4, p: false}
            ],
            b: "x",
            r: [
                {x: 1, y: "name_d", z: date_2, p: false},
                {x: 2, y: "name_a", z: date_1, p: true},
                {x: 4, y: "name_b", z: date_4, p: false},
                {x: 5, y: "name_c", z: date_3, p: true}
            ]
        },
        {
            a: [
                {x: 2, y: "name_a", z: date_1, p: true},
                {x: 1, y: "name_d", z: date_2, p: false},
                {x: 5, y: "name_c", z: date_3, p: true},
                {x: 4, y: "name_b", z: date_4, p: false}
            ],
            b: "y",
            r: [
                {x: 2, y: "name_a", z: date_1, p: true},
                {x: 4, y: "name_b", z: date_4, p: false},
                {x: 5, y: "name_c", z: date_3, p: true},
                {x: 1, y: "name_d", z: date_2, p: false}
            ]
        },
        {
            a: [
                {x: 2, y: "name_a", z: date_1, p: true},
                {x: 1, y: "name_d", z: date_2, p: false},
                {x: 5, y: "name_c", z: date_3, p: true},
                {x: 4, y: "name_b", z: date_4, p: false}
            ],
            b: "z",
            r: [
                {x: 2, y: "name_a", z: date_1, p: true},
                {x: 5, y: "name_c", z: date_3, p: true},
                {x: 1, y: "name_d", z: date_2, p: false},
                {x: 4, y: "name_b", z: date_4, p: false}
            ]
        },
        {
            a: [
                {x: 2, y: "name_a", z: date_1, p: true},
                {x: 1, y: "name_d", z: date_2, p: false},
                {x: 5, y: "name_c", z: date_3, p: true},
                {x: 4, y: "name_b", z: date_4, p: false}
            ],
            b: "p",
            r: [
                {x: 4, y: "name_b", z: date_4, p: false},
                {x: 1, y: "name_d", z: date_2, p: false},
                {x: 5, y: "name_c", z: date_3, p: true},
                {x: 2, y: "name_a", z: date_1, p: true}
            ]
        },
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return sorted version of the array, using '${tests[i].b}' as the sort key`, () => {
            expect(sortArrayBy(tests[i].a, tests[i].b)).toStrictEqual(tests[i].r);
        });
    }
})

describe('Method for finding the unique objects in an array of objects', () => {

    const date_1 = new Date(1900, 10, 1, 0, 0, 0, 0).toISOString()
    const date_2 = new Date(2022, 10, 2, 0, 0, 0, 0).toISOString()
    const date_3 = new Date(2022, 10, 1, 0, 0, 0, 0).toISOString()
    const date_4 = new Date(2023, 5, 10, 0, 0, 0, 0).toISOString()

    const tests: { a: (null | undefined | { x: number, y: string, z: string, p: boolean })[], r: number[] }[] = [
        {
            a: [
                {x: 2, y: "name_a", z: date_1, p: true},
                {x: 1, y: "name_d", z: date_2, p: false},
                {x: 5, y: "name_c", z: date_3, p: true},
                {x: 4, y: "name_b", z: date_4, p: false}
            ],
            r: [0, 1, 2, 3]
        },
        {
            a: [
                {x: 2, y: "name_a", z: date_1, p: true},
                {x: 1, y: "name_d", z: date_2, p: false},
                {x: 4, y: "name_b", z: date_4, p: false},
                {x: 4, y: "name_b", z: date_4, p: false}
            ],
            r: [0, 1, 2]
        },
        {
            a: [
                {x: 2, y: "name_a", z: date_1, p: true},
                {x: 2, y: "name_a", z: date_1, p: true},
                null,
                undefined
            ],
            r: [0]
        },
        {
            a: [
                {x: 2, y: "name_a", z: date_1, p: true},
                {x: 1, y: "name_d", z: date_2, p: false},
                {x: 1, y: "name_d", z: date_2, p: false},
                {x: 4, y: "name_b", z: date_4, p: false}
            ],
            r: [0, 1, 3]
        }
    ];

    for (let i = 0; i < tests.length; i++) {
        test(`should return '${JSON.stringify(tests[i].r)}' as the unique indices`, () => {
            expect(getUniqueObjectIndicesFromArray(tests[i].a)).toStrictEqual(tests[i].r);
        });
    }
})