/**
 * A group of utilities for processing arrays.
 * @category Utils
 * @module ArrayUtils
 */

import {convertKeyToText} from "./utils_string";
import type {DateFormat, DatetimeFormat, Option, TableRow} from "../../types/types_general";
import {hasOwnProperty, isDefined} from "./utils_trac_type_chckers";
import {objectsEqual} from "./utils_object";
import {setAnyFormat} from "./utils_formats";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * A function that checks if two arrays of objects are identical with the order mattering. A deep comparison of each object's
 * properties is done so this can take some time.
 *
 * @see https://stackoverflow.com/questions/27030/comparing-arrays-of-objects-in-javascript
 *
 * @param a1 - The first array to check.
 * @param a2 - The second array to check.
 */
export const arraysOfObjectsEqual = (a1: Record<string, any>[], a2: Record<string, any>[]): boolean => (

    a1.length === a2.length && a1.every((o, i) => objectsEqual(o, a2[i]))
)

/**
 * A function that checks if two arrays of objects are identical without the order mattering. A deep comparison of each object's
 * properties is done using a key to locate the object in each array to compare. There is a performance optimisation from
 * checking if the keys in the object's being compared are the same length.
 *
 * @see https://stackoverflow.com/questions/27030/comparing-arrays-of-objects-in-javascript
 *
 * @param a1 - The first array to check.
 * @param a2 - The second array to check.
 * @param key - The property of the objects in the array to use to find the object in each array to compare. This must be a
 * property that all the objects have. In a TRAC schema this could be 'fieldName' for example when trying to compare two schemas.
 */
export const arraysOfObjectsEqualByKey = (a1: Record<string, any>[], a2: Record<string, any>[], key: string): boolean => (

    a1.length === a2.length && arraysOfPrimitiveValuesEqual(a1.map(item => item[key]), a2.map(item => item[key])) && a1.every((o) => objectsEqual(o, a2.find(item => item[key] === o[key]) || {}))
)

/**
 * A function that checks if two arrays of primitive values are identical without the order mattering.
 *
 * @param a1 - The first array to check.
 * @param a2 - The second array to check.
 */
export const arraysOfPrimitiveValuesEqual = (a1: (boolean | null | undefined | string | number)[], a2: (boolean | null | undefined | string | number)[]): boolean => (

    !(a1.length !== a2.length || a1.some((o) => !a2.includes(o)))
)

/**
 * A function that checks if one array of primitive values is a subset of another without the order mattering.
 *
 * @param a1 - The first array to check, this is the smaller subset.
 * @param a2 - The second array to check, this is the larger superset.
 */
export const arraysOfPrimitiveValuesASubset = (a1: (boolean | null | undefined | string | number)[], a2: (boolean | null | undefined | string | number)[]): boolean => (

    !a1.some((o) => !a2.includes(o))
)

/**
 * A function that checks if two arrays of primitive values are identical with the order mattering.
 *
 * @param a1 - The first array to check.
 * @param a2 - The second array to check.
 */
export const arraysOfPrimitiveValuesEqualInOrder = (a1: (boolean | null | undefined | string | number)[], a2: (boolean | null | undefined | string | number)[]): boolean => (

    !(a1.length !== a2.length || a1.some((o, i) => o !== a2[i]))
)

/**
 * A function that takes an array of strings and counts how many of each entry there is. Note that the
 * keys of the returned object will be uppercase versions of the values in 'myArray' when caseSensitive is set to
 * true.
 *
 * @param myArray - The array of strings to count the unique elements of.
 * @param caseSensitive - Whether the count should be case-insensitive.
 */
export const countItemsInArray = (myArray: string[], caseSensitive: boolean = false): Record<string, number> => {

    let accumulator: { [key: string]: number } = {}

    return myArray.reduce((a, b) => ({
        ...a,
        [!caseSensitive ? b.toUpperCase() : b]: (a[!caseSensitive ? b.toUpperCase() : b] || 0) + 1
    }), accumulator)
}

/**
 * A function that returns a list of items in an array that are duplicated.
 *
 * @param myArray - The array os strings to get the duplicates elements of.
 * @param caseSensitive - Whether the count should be case-insensitive.
 */
export const duplicatesInArray = (myArray: string[], caseSensitive: boolean = false): string[] => {

    const count = countItemsInArray(myArray, caseSensitive)
    return Object.keys(count).filter(a => count[a] > 1)
}

/**
 * A function that takes an array of objects and makes the array unique by each object. This does a full
 * deep comparison of each object.
 *
 * @see https://stackoverflow.com/questions/2218999/how-to-remove-all-duplicates-from-an-array-of-objects
 *
 * @param myArray - The array to make unique.
 * @returns An array where each object is unique.
 */
export const makeArrayOfObjectsUnique = <U extends Object[]>(myArray: U): U => {

    return myArray.filter((obj1, index, self) =>
            index === self.findIndex((obj2) => (
                objectsEqual(obj1, obj2)
            ))
    ) as U
}

/**
 * A function that takes an array of objects and makes the array unique by a property in the object. Note that
 * properties other than the one specified by the property argument can be duplicated across the objects.
 *
 * @param myArray - The array to make unique.
 * @param key - The property to make the array unique by.
 * @returns An array where each object is unique by the value of the property defined by 'property'.
 */
export const makeArrayOfObjectsUniqueByProperty = <U extends Object[]>(myArray: U, key: string): U => {

    // The second hasOwnProperty is redundant but the only way I could think to making Typescript happy
    // property is a key of the object
    return myArray.filter((obj1, index, self) =>
            hasOwnProperty(obj1, key) && self.filter(
                obj2 => hasOwnProperty(obj2, key)
            ).map(
                obj2 => hasOwnProperty(obj2, key) ? obj2[key] : "never"
            ).indexOf(obj1[key]) === index
    ) as U
}


/**
 * A function that parses an array of objects, such as a dataset, and extracts rows that
 * have a unique combinations of variables set by the keys array.
 *
 * @param myArray - The array to get the unique permutations from.
 * @param keys - The properties to get the combinations of.
 * @returns An array where each object is a unique combination of the variables defined by 'keys'.
 */
export const getUniqueCombinationsOfPropertiesFromArray = <U extends Object>(myArray: U[], keys: (keyof U)[]): Partial<U>[] => {

    // This will be the set of unique combinations found
    const uniqueKeys = new Set()

    return myArray.reduce((acc: Partial<U>[], row) => {

        // Create a string version of the key
        const key = keys.map(key => row[key]).join("-")

        // If the key exists in the set then return the accumulator
        if (uniqueKeys.has(key)) return acc

        // Otherwise, add the key to the list of unique keys and add the row to the accumulator
        uniqueKeys.add(key)

        // Keep only the properties that make up the key
        let newRow: Partial<U> = {}
        keys.forEach(key => {
            newRow[key] = row[key]
        })

        return [...acc, newRow]

    }, [])
}

/**
 * A function that takes an array of objects and finds the minimum and maximum values of a given property or column.
 * This is used for example when adding heat maps to a column, and you need to know the minimum and maximum values in
 * order to set a ranking.
 *
 * The function can handle finding the minimum and maximum of numbers, booleans and strings, in the latter two cases
 * false < true and strings are ordered alphabetically.
 *
 * @remarks
 * This typying of this function means that the key property must be present in the object since undefined is not
 * an allowed value in the object.
 *
 * @param myArray - The array of objects to search.
 * @param key - The key or column to get the minimum and maximum values of.
 */
export const getMinAndMaxValueFromArrayOfObjects = (myArray: TableRow[], key: string): { minimum: undefined | number | boolean | string | Date, maximum: undefined | number | boolean | string | Date } => {

    let result: { minimum: undefined | number | boolean | string | Date, maximum: undefined | number | boolean | string | Date } = {
        minimum: undefined,
        maximum: undefined
    }

    // Go through the entire array and calculate the minimum and maximum non-null values. Ote that we do not allow this
    // to be done for string data except that dates are stored as strings but because these order when compared we
    // will still get the min and max dates correctly.
    return myArray.reduce((result, row) => {

        // null values are allowed in the row data but undefined are not
        if (row.hasOwnProperty(key)) {

            const value = row[key]

            if (value !== null && !Array.isArray(value)) {

                // We don't use Math.min and Math.max as these do not support Date objects
                if ((result.minimum === undefined || value < result.minimum)) {
                    result.minimum = value
                }
                if (result.maximum === undefined || value > result.maximum) {
                    result.maximum = value
                }
            }
        }

        return result

    }, result)
}

/**
 * A function that converts an array of primitive values to a set of options that can be used in the {@link SelectOption} component.
 * It can optionally make the options more easily readable by trying to make the labels more human-readable. The initial array is
 * deduplicated before the conversion into options.
 *
 * @param myArray - The array to convert into options.
 * @param alterText - Whether to make the label human-readable.
 * @param basicType - The TRAC basic type of items in the array, this is needed if the labels need to have a format applied.
 * @param formatCodeAsString - The numeric format to apply if the array values if basicType is set e.g. ",|.|2|£||1".
 * @returns A set of options for the SelectOption component.
 */
export function convertArrayToOptions<T extends string | null | number | boolean>(myArray: T[], alterText: boolean = true, basicType?: trac.BasicType, formatCodeAsString?: null | string | DatetimeFormat | DateFormat): Option<T>[] {

    // Note the (key && alterText) condition - this is to cope with null values which are legitimate options.
    return [...new Set(myArray)].map(key => ({
        value: key,
        // Labels must be strings
        label: key == null || key === "" ? 'None' : (alterText && typeof key === "string") ? convertKeyToText(key) : (basicType !== undefined && formatCodeAsString != undefined) ? setAnyFormat(basicType, formatCodeAsString, key) ?? key.toString() : key.toString()
    })) as Option<T>[]
}

/**
 * A function that sorts and array of objects by a property of each object. The typescript says that the output array
 * has the same type as the input array but that the array items must be an object with string keys.
 *
 * @typeParam U - The type of the objects in myArray.
 * @param myArray - The array to sort.
 * @param propertyToSortBy - The property of each object to sort by.
 * @returns The sorted array.
 */
export const sortArrayBy = <U extends Record<string, any>>(myArray: U[], propertyToSortBy: keyof U): typeof myArray => {

    // We need to slice to copy the array because myArray could have read only elements i.e. it could come from a store
    return myArray.slice().sort((a, b) => (typeof a[propertyToSortBy] === "string" && typeof b[propertyToSortBy] === "string" ? (a[propertyToSortBy].toUpperCase() > b[propertyToSortBy].toUpperCase() ? 1 : -1) : a[propertyToSortBy] > b[propertyToSortBy] ? 1 : -1))
}

/**
 * A function that takes an array of objects, null or undefined values and finds the indices of the unique objects in the
 * array. Array elements that are null or undefined are removed and their indices are not returned.
 *
 * @see https://stackoverflow.com/questions/2218999/how-to-remove-all-duplicates-from-an-array-of-objects
 *
 * @param myArray - The array to get the indices of the unique objects of.
 * @returns - The array of indices for the unique objects.
 */
export const getUniqueObjectIndicesFromArray = <U extends (Object | undefined | null)[]>(myArray: U): number[] => {

    return makeArrayOfObjectsUnique(myArray.filter(isDefined)).map(obj1 => {

        return myArray.filter(isDefined).findIndex(obj2 => objectsEqual(obj1, obj2))
    })
}

/**
 * A function that takes an array, which could be a very large dataset, and randomly samples a set number
 * of rows from it without duplication. This is useful when plotting charts where the underlying data is
 * too large to be plotted.
 *
 * @remarks I spent a whole evening trying to work out what this does, it's super clever but seriously save yourself some time.
 *
 * @see https://stackoverflow.com/questions/19269545/how-to-get-a-number-of-random-elements-from-an-array
 *
 * @param myArray - The array to sample from.
 * @param sample - The number of rows to sample.
 * @param keepSameOrder - Whether the order of the original array needs to be preserved. There is a
 * performance hit to applying this.
 */
export const randomSampleFromArray = (myArray: any[], sample: number, keepSameOrder: boolean) => {

    // An empty array as big as the sample we are going to take
    let result = new Array(sample),
        len = myArray.length,
        // An empty array as big as the full array to sample from
        taken = new Array(len);

    // This is used to resort the random array back into its original order.
    let order: { order: number, index: number }[] = []

    if (sample > len) {
        throw new RangeError("randomSampleFromArray: more elements taken than available");
    }

    while (sample--) {

        let x = Math.floor(Math.random() * len);

        // If the random number is not in the taken array (already been generated) then add a copy of the full array value the result.
        // The 'in' operator is checking if the index is populated not the value, i.e. have we already selected this value. If
        // we have taken the item before then we take from the end of the original array, moving inwards each loop.
        result[sample] = myArray[x in taken ? taken[x] : x];
        // Each loop len goes down by one from the size of the full array -1

        // Index tells us two things, we know two things, what random numbers we generated and
        // what index we copied from. If we generate the same random number again then we lose some info because we
        // replace the value at the index corresponding to the random number with what index we copied from.

        // I think that the 'len in taken' check is saying that we generated a new random number differently to any before
        // but that if we did have to use the fallback copied from the end then we would have copied the same one as one
        // already copied across. It signifies a clash between the random selections and the backup you have, if taken
        // includes duplicated values I think it is a sign that the selection is only quasi random.

        taken[x] = --len in taken ? taken[len] : len;

        if (keepSameOrder) order.push({order: x in taken ? taken[x] : x, index: order.length})
    }

    return keepSameOrder ? sortArrayBy(order, "order").map(item => result[item.index]) : result
}