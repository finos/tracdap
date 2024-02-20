import {MultiValue, SingleValue} from "react-select";
import {
    AsString,
    BasicTypesString,
    ColourColumnTypes,
    DateFormat,
    DatetimeFormat,
    GenericGroup,
    GenericOption,
    ObjectTypesString,
    Option,
    Position,
    ThemesList,
    Variants
} from "../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {DateFormats} from "../../config/config_general";
import {isValidIsoDateString, isValidIsoDatetimeString} from "./utils_string";
import {Endpoints} from "@octokit/types";

/**
 * See https://stackoverflow.com/questions/43010737/way-to-tell-typescript-compiler-array-prototype-filter-removes-certain-types-fro
 */

export function isDefined<T>(argument: T | null | undefined): argument is T {
    return argument != null
}

//https://stackoverflow.com/questions/64373085/type-guard-that-verifies-a-generic-objects-properties-are-all-not-optional-null
type PropNonNullable<T, TKey extends keyof T> = T & { [P in TKey]-?: NonNullable<T[P]> };
export const valueNotNull = <T, TKey extends keyof T>(key: TKey) => (vals: T): vals is PropNonNullable<T, TKey> =>
    vals != null && vals[key] != null;

export function isTracNumber(argument: any): argument is (trac.BasicType.INTEGER | trac.BasicType.FLOAT | trac.BasicType.DECIMAL) {
    return trac.INTEGER === argument || trac.FLOAT === argument || trac.DECIMAL === argument
}

export function isTracBasicType(argument: any): argument is trac.BasicType {
    return Object.values(trac.metadata.BasicType).includes(argument)
}

export function isTracBasicTypeString(argument: any): argument is BasicTypesString {
    return Object.keys(trac.BasicType).includes(argument)
}

export function isTracObjectType(argument: any): argument is trac.metadata.ObjectType {
    return Object.values(trac.metadata.ObjectType).includes(argument)
}

export function isTracObjectTypeString(argument: any): argument is ObjectTypesString {
    return Object.keys(trac.metadata.ObjectType).includes(argument)
}

export function isTracDateOrDatetime(argument: any): argument is (trac.BasicType.DATE | trac.BasicType.DATETIME) {
    return trac.DATE === argument || trac.DATETIME === argument
}

export function isTracFormatable(argument: any): argument is (trac.BasicType.DATE | trac.BasicType.DATETIME | trac.BasicType.INTEGER | trac.BasicType.FLOAT | trac.BasicType.DECIMAL) {
    return isTracDateOrDatetime(argument) || isTracNumber(argument)
}

export function isTracString(argument: any): argument is (trac.BasicType.STRING) {
    return trac.STRING === argument
}

export function isTracBoolean(argument: any): argument is (trac.BasicType.BOOLEAN) {
    return trac.BOOLEAN === argument
}

/**
 * A function that acts as a type guard that asserts whether a select component value is an option.
 *
 * @param argument - The value to test.
 */
export function isOption(argument: MultiValue<Option> | Option | string | boolean | null | number | undefined | unknown): argument is Option {

    return isObject(argument) && argument.hasOwnProperty("value") && argument.hasOwnProperty("label")
}

/**
 * A function that acts as a type guard that asserts whether a select component value is an option with a string value.
 *
 * @param argument - The value to test.
 */
export function isStringOption(argument: MultiValue<Option> | Option | string | boolean | null | number | undefined | unknown): argument is Option<string> {

    return isOption(argument) && typeof argument.value === "string"
}

/**
 * A function that acts as a type guard that asserts whether a select component value is an option with an integer value.
 *
 * @param argument - The value to test.
 */
export function isIntegerOption(argument: MultiValue<Option> | Option | string | boolean | null | number | undefined | unknown): argument is Option<number> {

    return isOption(argument) && typeof argument.value === "number" && Math.round(Number(argument.value)) === argument.value
}

/**
 * A function that acts as a type guard that asserts whether a select component value is an option with a number value.
 *
 * @param argument - The value to test.
 */
export function isNumberOption(argument: MultiValue<Option> | Option | string | boolean | null | number | undefined | unknown): argument is Option<number> {

    return isOption(argument) && typeof argument.value === "number"
}

/**
 * A function that acts as a type guard that asserts whether a select component value is an option with a boolean value.
 *
 * @param argument - The value to test.
 */
export function isBooleanOption(argument: MultiValue<Option> | Option | string | boolean | null | number | undefined | unknown): argument is Option<boolean> {

    return isOption(argument) && typeof argument.value === "boolean"
}

/**
 * A function that acts as a type guard that asserts whether a select component value is an option with a string value that is a valid ISO date.
 *
 * @param argument - The value to test.
 */
export function isDateOption(argument: MultiValue<Option> | Option | string | boolean | null | number | undefined | unknown): argument is Option<string> {

    return isOption(argument) && typeof argument.value === "string" && isValidIsoDateString(argument.value)
}

/**
 * A function that acts as a type guard that asserts whether a select component value is an option with a string value that is a valid ISO datetime.
 *
 * @param argument - The value to test.
 */
export function isDatetimeOption(argument: MultiValue<Option> | Option | string | boolean | null | number | undefined | unknown): argument is Option<string> {

    return isOption(argument) && typeof argument.value === "string" && isValidIsoDatetimeString(argument.value)
}

export function isMultiOption(argument: MultiValue<Option> | Option | string | boolean | null | number): argument is MultiValue<Option> {

    return !(isOption(argument) || typeof argument === "string" || typeof argument === "boolean" || typeof argument === "number" || argument == null)
}

export function isDateFormat(argument: string | null | undefined): argument is DateFormat | DatetimeFormat {
    return argument != null && Object.keys(DateFormats).includes(argument)
}

/**
 * A function that checks if a variable is an object or not.
 * See https://stackoverflow.com/questions/8511281/check-if-a-value-is-an-object-in-javascript
 * @param myObject -
 */
export function isObject(myObject: any): myObject is Record<string, any> {

    return typeof myObject === "object" && myObject !== null
}

/**
 * A type checker that asserts whether the argument is of type trac.metadata.IValue.
 * @param argument - The variable to test.
 */
export function isValue(argument: any): argument is trac.metadata.IValue {

    const valueKeys = ["booleanValue", "integerValue", "floatValue", "stringValue", "decimalValue", "dateValue", "datetimeValue", "arrayValue", "mapValue"]

    return isObject(argument) && Object.keys(argument).some(key => valueKeys.includes(key))
}

export function isITagUpdate(argument: any): argument is trac.metadata.ITagUpdate {
    return argument != null && isObject(argument) && Object.keys(argument).includes("attrName") && isValue(argument.value)
}

export function isType<T>(argument: T | null | undefined): argument is T {
    return argument != null
}

/**
 * See https://fettblog.eu/typescript-hasownproperty/
 * @param obj
 * @param prop
 */
// export function hasOwnProperty<X extends {} | null | undefined, Y extends PropertyKey>(obj: X, prop: Y): obj is X & Record<Y, X[keyof X]> {
export function hasOwnProperty<X extends {} | null | undefined, Y extends PropertyKey>(obj: X, prop: Y): obj is X & Record<Y, unknown> {

    return obj != null && prop != null && obj.hasOwnProperty(prop)
}

export function assertOwnProperty<T, K extends keyof T>(obj: T, property: K) {
    if (typeof obj === "object" && !(obj as Object).hasOwnProperty(property)) {
        throw new Error(`Expected object to have property`)
    }
}

// has own property on steroids https://stackoverflow.com/questions/69195889/typeguard-function-for-a-key-in-a-generic-object
type RequireLiteral<K extends PropertyKey> =
    string extends K ? never :
        number extends K ? never :
            symbol extends K ? never :
                K

export function has<T extends object, K extends PropertyKey>(
    obj: T,
    property: RequireLiteral<K>
): obj is T & { [P in K]: { [Q in P]: unknown } }[K];
export function has(obj: any, property: PropertyKey): boolean;
export function has(obj: any, property: PropertyKey) {
    return Object.prototype.hasOwnProperty.call(obj, property)
}

export const isKeyOf =
    <ObjectType extends null | undefined | Record<PropertyKey, unknown>>(object: ObjectType, property: null | undefined | PropertyKey): property is keyof ObjectType => {
        return object != null && property != null && Object.prototype.hasOwnProperty.call(object, property);
    };


export function isSingleValue(argument: SingleValue<Option> | MultiValue<Option>): argument is SingleValue<Option> {
    return argument != undefined && !Array.isArray(argument)
}

export function isMultiValue(argument: SingleValue<Option> | MultiValue<Option>): argument is SingleValue<Option> {
    return Array.isArray(argument)
}

// // See https://fettblog.eu/typescript-array-includes/
// function isBasicType<T extends U, U>(el: U): el is trac.BasicType {
//
//     type Keys = keyof typeof trac.BasicType;
//     type Values = typeof trac.BasicType[Keys];
//
//   return Values.includes(el as T);
// }

export function isPrimitive(argument: any): argument is null | string | number | boolean {

    return argument === null || ["string", "number", "boolean"].includes(typeof argument)
}

/**
 * A function that type checks whether a string is a valid theme name.
 * @param myString - The string to check.
 */
export function isThemesList(myString: undefined | string): myString is ThemesList {

    return myString != null && ["lightTheme", "darkTheme", "clientTheme"].includes(myString);
}

export function isColumnColourType(myString: undefined | string | ColourColumnTypes): myString is ColourColumnTypes {

    return myString != null && ["none", "heatmap", "trafficlight"].includes(myString);

}

/**
 * A function that type checks whether a string is a valid Bootstrap variant name supported by the Alert component.
 * @param myString - The string to check.
 */
export function isAlertVariant(myString: undefined | string): myString is Extract<Variants, "success" | "warning" | "danger" | "info"> {

    return myString != null && ["success", "warning", "danger", "info"].includes(myString);
}

// See https://stackoverflow.com/questions/69026336/user-defined-type-guards-for-array-element-type
export const isStringArray = (arg: any): arg is string[] => {
    return Array.isArray(arg) && arg.every(item => typeof item === 'string')
}

export const isBooleanArray = (arg: any): arg is boolean[] => {
    return Array.isArray(arg) && arg.every(item => typeof item === 'boolean')
}

export const isNumberArray = (arg: any): arg is number[] => {
    return Array.isArray(arg) && arg.every(item => typeof item === 'number')
}


//See https://fettblog.eu/typescript-array-includes/
export function includes<T extends U, U>(coll: ReadonlyArray<T>, el: U): el is T {
    return coll.includes(el as T);
}

export function isGroupOption(arg: GenericOption[] | GenericGroup[]): arg is GenericGroup[] {

    return !arg.some(tier => !isObject(tier) || !hasOwnProperty(tier, "options") || !Array.isArray(tier.options))
}

/**
 * A function that checks if a variable is a data object or not.
 * See https://stackoverflow.com/questions/8511281/check-if-a-value-is-an-object-in-javascript
 * @param variable -
 */
export const isDateObject = (variable: any): variable is Date => {

    return Object.prototype.toString.call(variable) === '[object Date]'
}

/**
 * A function that checks if a variable is an option for the SelectOption component and whether it has the asString extended property.
 * This is needed as sometimes you get a payload which can be any number of the Option types and need to assess if it is an AsString
 * version for a particular if clause.
 * @param argument -
 */
export function isAsStringOption(argument: any): argument is Option<void, AsString> {
    return isOption(argument) && hasOwnProperty(argument, "details") && hasOwnProperty(argument.details, "asString") && (argument.details.asString === null || typeof argument.details.asString === "string")
}

/**
 * A function that checks if a variable is an option for the SelectOption component and whether it has the tag extended property.
 * This is needed as sometimes you get a payload which can be any number of the Option types and need to assess if it is a
 * Tag or TagHeader type version for a particular if clause.
 * @param argument -
 */
export function isTagOption(argument: any): argument is Option<string, trac.metadata.ITag | trac.metadata.Tag> {
    return isOption(argument) && hasOwnProperty(argument, "tag") && hasOwnProperty(argument.tag, "header") && isObject(argument.tag.header) && typeof argument.value === "string"
}

export function isTagOptionArray(argument: any[]): argument is Option<string, trac.metadata.ITag | trac.metadata.Tag>[] {
    return !argument.some(option => !isTagOption(option))
}


/**
 * A function that checks if a variable is an option for the SelectOption component and whether it has the position extended property.
 * This is needed as sometimes you get a payload which can be any number of the Option types and need to assess if it is a
 * Position type version for a particular if clause.
 * @param argument - The variable to test.
 */
export function isPositionOption(argument: any): argument is Option<void, Position> {
    return isOption(argument) && hasOwnProperty(argument, "details") && hasOwnProperty(argument.details, "position") && typeof argument.details.position === "string"
}

/**
 * A function that checks if a variable is an option for the SelectOption component and whether it has the type extended property.
 * This is needed as sometimes you get a payload which can be any number of the Option types and need to assess if it is a
 * Position type version for a particular if clause.
 * @param argument - The variable to test.
 */
export function isTypeOption(argument: any): argument is Option<void, trac.BasicType | trac.ObjectType | trac.FlowNodeType> {
    return isOption(argument) && hasOwnProperty(argument, "type") && typeof argument.type === "number"
}

/**
 * A function that checks is a variable is a valid DatetimeValue from the TRAC metadata.
 * @param argument - The argument to test.
 */
export function isDatetimeValue(argument: any): argument is trac.metadata.IDatetimeValue {

    return isObject(argument) && argument.hasOwnProperty("isoDatetime") && (typeof argument.isoDatetime === "string" || argument.isoDatetime === null)
}

/**
 * A function that checks if an object is a GitHub commit. This is used when loading a file from GitHub and you are choosing
 * from either a commit or a release, these two items have different interfaces and to be able to use them in Typescript we
 * need to be able to establish which type of object has been selected.
 *
 * @param object - The argument to test.
 */
export function isGitHubCommit(object: any): object is Endpoints["GET /repos/{owner}/{repo}/commits"]["response"]["data"][number] {

    return object && isObject(object) && object.hasOwnProperty("identifier") && object.identifier === "commit";
}