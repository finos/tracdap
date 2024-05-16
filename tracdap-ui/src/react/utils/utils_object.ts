/**
 * A group of utilities for processing objects.
 * @category Utils
 * @module ObjectUtils
 */
import {isObject} from "./utils_trac_type_chckers";
import {Option} from "../../types/types_general";
import {convertKeyToText} from "./utils_string";

/**
 * A function that checks if two objects are identical using a deep comparison of the object properties.
 *
 * @see https://stackoverflow.com/questions/27030/comparing-arrays-of-objects-in-javascript
 *
 * @param o1 - The first object to check.
 * @param o2 - The second object to check.
 */
export const objectsEqual = (o1: Record<string, any>, o2: Record<string, any>): boolean => {

    // If both objects are {} then just return true
    if (isObject(o1) && isObject(o2) && Object.keys(o1).length === 0 && Object.keys(o2).length === 0) {
        return true
    }

    // Note that the function is recursive, so we need to check the type is an object or not
    return isObject(o1) && isObject(o2) && Object.keys(o1).length > 0
        ? Object.keys(o1).length === Object.keys(o2).length
        && Object.keys(o1).every(p => objectsEqual(o1[p], o2[p]))
        : o1 === o2;
}

/**
 * A function that converts the keys of an object to a set of options that can be used in the {@link SelectOption} component. It can
 * optionally make the options more easily readable.
 *
 * @param myObject - The object to convert into options.
 * @param alterText - Whether to make the value uppercase and the label human-readable.
 * @returns An array of options for use in the SelectOption component.
 */
export const convertObjectKeysToOptions = (myObject: Record<string,  any>, alterText: boolean = true): Option<string>[] => {

    // Note the (key && alterText) condition - this is to cope with null values which are legitimate options.
    return Object.keys(myObject).map(key => ({
        value: (key && alterText) ? key.toUpperCase() : key,
        label: alterText ? convertKeyToText(key) : key.toString()
    }))
}

/**
 * A function that generates a unique key for an object. An array of properties of the object is provided that when extracted and
 * concatenated uniquely identify the object. This can be used for example when trying to store the keys relating to which rows
 * in a dataset have been selected by the user.
 *
 * @param myObject - The object to generate the unique key for.
 * @param uniqueKeyVariables - The properties of the object that uniquely define it.
 * @returns A string that can be used as a unique reference to the object.
 */
export const getUniqueKeyFromObject = (myObject: Record<string, any>, uniqueKeyVariables: string[]): string => (

    Object.entries(myObject).filter(([fieldName]) => uniqueKeyVariables.includes(fieldName)).map(([, value]) => value).join("-")
)

/**
 * A function that checks if any property of an object is a particular value. This is used for example when storing attributes about
 * a dataset (such as whether an element is valid) and we use this function to see if anything in the dataset is invalid.
 *
 * @remarks
 * This function only accepts an object as the 'myObject' argument but in order to do a deep check whether the value exists anywhere
 * in the object and not just at the first level down we have an internal function which replicates this function's logic but the
 * interface for 'myObject' is broader so that it can handle primitive types as the argument.
 *
 * @param myObject - The object to check.
 * @param value - The value to check for.
 */
export const objectsContainsValue = (myObject: Record<string | number, string | number | boolean | {}>, value: string | number | boolean | {}): boolean => {

    function _objectsContainsValue(myObject: string | number | boolean | Record<string | number, string | number | boolean | {}>, value: string | number | boolean | {}): boolean {

        return isObject(myObject) && Object.keys(myObject).length > 0
            ? Object.keys(myObject).some(p => _objectsContainsValue(myObject[p], value))
            : myObject === value;
    }

    return isObject(myObject) && Object.keys(myObject).length > 0
        ? Object.keys(myObject).some(p => _objectsContainsValue(myObject[p], value))
        : isObject(myObject) && Object.keys(myObject).length === 0 ? false : myObject === value;
}

/**
 * A function that returns a deep nested property from an object using an array as a
 * path to the property.
 *
 * @see https://hackernoon.com/accessing-nested-objects-in-javascript-f02f1bd6387f
 *
 * @param myObject - The object to get the nested item from.
 * @param pathArray - The array of properties to navigate through to object to.
 * @returns The value at the property specified by pathArray or undefined if it does not exist.
 */
export const getNestedObject = (myObject: Record<string, any>, pathArray: string[]): unknown => {

    return pathArray.reduce((obj, key) => (obj && obj[key] !== 'undefined') ? obj[key] : undefined, myObject);
}

/**
 * A function that returns an array of the names of properties containing string values.
 * @param myObject - The object to get the properties from.
 * @returns An array of boolean properties.
 */
export const getBooleanProperties = (myObject: any): string[] => {

    return !isObject(myObject) ? [] : Object.entries(myObject).filter(([, value]) => {
        return typeof value === "boolean"
    }).map(([key]) => key)
}

/**
 * A function that returns an array of the names of properties containing string values.
 * @param myObject - The object to get the properties from.
 * @returns An array of number properties.
 */
export const getNumberProperties = (myObject: any): string[] => {

    return !isObject(myObject) ? [] : Object.entries(myObject).filter(([, value]) => {
        return typeof value === "number"
    }).map(([key]) => key)
}

/**
 * A function that returns an array of the names of properties containing string values.
 * @param myObject - The object to get the properties from.
 * @returns An array of string properties.
 */
export const getStringPropertiesAsArray = (myObject: any): string[] => {

    return !isObject(myObject) ? [] : Object.entries(myObject).filter(([, value]) => {
        return typeof value === "string"
    }).map(([key]) => key)
}

/**
 * A function that returns an array of the names of properties containing arrays of string
 * values.
 *
 * @param myObject - The object to get the properties from.
 * @returns An array of string properties that are string arrays.
 */
export const getStringArrayProperties = (myObject: any): string[] => {

    return !isObject(myObject) ? [] : Object.entries(myObject).filter(([, value]) => {
        return Array.isArray(value) && value.every(item => typeof item === "string")
    }).map(([key]) => key)
}