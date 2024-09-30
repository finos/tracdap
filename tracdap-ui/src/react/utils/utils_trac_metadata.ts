/**
 * A group of utilities for processing TRAC metadata.
 *
 * @category Utils
 * @module TracMetadataUtils
 */

import {DataValues, DeepWritable, ExtractedTracValue, Option, SelectPayload} from "../../types/types_general";
import {
    hasOwnProperty,
    isBooleanOption,
    isDateOption,
    isDatetimeOption,
    isDatetimeValue,
    isDefined,
    isGroupOption,
    isIntegerOption,
    isITagUpdate,
    isKeyOf,
    isNumberOption,
    isObject,
    isPrimitive,
    isStringOption,
    isTracBoolean,
    isTracFormatable,
    isTracNumber,
    isTracObjectType,
    isTracString,
    isValue
} from "./utils_trac_type_chckers";
import {getGroupOptionIndex} from "./utils_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {UiAttributesProps} from "../../types/types_attributes_and_parameters";
import {getAttributesByObject} from "./utils_attributes_and_parameters";
import {convertKeyToText, convertStringToInteger, isObjectId, isObjectKey, isValidNumberAsString} from "./utils_string";
import {getNestedObject} from "./utils_object";
import {convertIsoDateStringToFormatCode, setAnyFormat} from "./utils_formats";


/**
 * A function that converts a TRAC basicType to its string equivalent, for example if value is an integer then it's
 * objectType will be 2. This function converts this number to 'integer' so that it is more human-readable. There are
 * some additional options on the returned string.
 *
 * @param basicType - The object type to convert to a string.
 * @param lower - Whether to make the string lower case.
 * @returns The basic type as a string.
 *
 * @example
 * console.log(convertBasicTypeToString(trac.INTEGER)) // "integer"
 *
 * @example
 * console.log(convertBasicTypeToString(trac.STRING, false)) // "STRING"
 */
export const convertBasicTypeToString = (basicType: trac.BasicType, lower: boolean = true): string => (

    lower ? trac.BasicType[basicType].toLowerCase() : trac.BasicType[basicType]
)

/**
 * A function that converts a TRAC objectType to its string equivalent, for example if an object is a model then it's
 * objectType will be 2. This function converts this number to 'model' so that it is more human-readable. There are
 * some additional options on the returned string.
 *
 * @param objectType - The object type to convert to a string.
 * @param lower - Whether to make the string lower case.
 * @param addSet - Whether to make a data objectType return 'dataset' instead of 'data'.
 * @returns The object type as a string.
 *
 * @example
 * console.log(convertObjectTypeToString(trac.MODEL)) // "model"
 *
 * @example
 * console.log(convertObjectTypeToString(trac.DATA, false, true)) // "DATASET"
 */
export const convertObjectTypeToString = (objectType: trac.ObjectType, lower: boolean = true, addSet: boolean = false): string => {

    const set: "set" | "" = addSet && objectType === trac.ObjectType.DATA ? "set" : ""

    return (lower ? trac.ObjectType[objectType].toLowerCase() : trac.ObjectType[objectType]) + set
}

/**
 * A function that converts a TRAC partType to its string equivalent, for example if a partition is a part by type
 * then it's partType will be 2. This function converts this number to 'part_by_value' so that it is more human-readable.
 * There are some additional options on the returned string.
 *
 * @param partType - The part type to convert to a string.
 * @param lower - Whether to make the string lower case.
 * @returns The part type as a string.
 *
 * @example
 * console.log(convertPartTypeToString(trac.PART_BY_VALUE)) // "part_by_value"
 *
 * @example
 * console.log(convertPartTypeToString(trac.PART_BY_RANGE, false)) // "PART_BY_RANGE"
 */
export const convertPartTypeToString = (partType: trac.metadata.PartType, lower: boolean = true): string => (

    lower ? trac.metadata.PartType[partType].toLowerCase() : trac.metadata.PartType[partType][partType]
)

/**
 * A function that converts a flow node type to a humanreadable string, this is used in messages to the user.
 * Unlike other functions for object types and basic types this uses a bespoke naming convention, it is used
 * for example in the {@link FlowSvg} component key.
 *
 * @param flowNodeType - The TRAC flow node type.
 * @returns The human-readable name for the node type.
 */
export const convertFlowNodeTypeToString = (flowNodeType: trac.metadata.FlowNodeType): string => {

    const flowNodeTypeLookup = {
        [trac.metadata.FlowNodeType.NODE_TYPE_NOT_SET]: "intermediate dataset",
        [trac.metadata.FlowNodeType.MODEL_NODE]: "model",
        [trac.metadata.FlowNodeType.INPUT_NODE]: "input dataset",
        [trac.metadata.FlowNodeType.OUTPUT_NODE]: "output"
    }

    return flowNodeTypeLookup[flowNodeType]
}

/**
 * A function that extracts a value from a TRAC objects metadata. This is designed to be used on the attrs and model
 * parameters. The function also returns the basicType and subBasicType (when the basicType is an array or a map). If
 * you have a tag from TRAC and there is an attribute you want to extract then pass the attribute to this function. If
 * it is an array attribute then this function will return an object with the array as the value property.
 *
 * @remarks
 * There is a sister function to this called {@link setTracValue} that does the reverse operation, it
 * receives a TRAC value object and extracts the value from it.
 *
 * @param tracObject - Either an attribute from the attrs property of a TRAC Tag or an object from the
 * header property of a TRAC Tag. These have slightly different structures but the underlying value can
 * be extracted in each case.
 *
 * @example
 * const results = extractValueFromTracValueObject({type: {basicType: trac.BasicType.INTEGER}, integerValue: null})
 * console.log(results) // {"basicType": 2, "subBasicType": undefined, "value": null}
 *
 * @example
 * const results = extractValueFromTracValueObject({type: {basicType: trac.BasicType.DATE}, dateValue: {isoDate: "2012-10-12"}})
 * console.log(results) // {"basicType": 6, "subBasicType": undefined, "value": "2012-10-12"}
 *
 * @example
 * const results = extractValueFromTracValueObject({type: {basicType: trac.BasicType.ARRAY}, arrayValue: {items: [{type: {basicType: trac.BasicType.STRING}, stringValue: "test string"}]}})
 * console.log(results) // {"basicType": 8, "subBasicType": 4, "value": ["test string"]}
 */
export const extractValueFromTracValueObject = (tracObject: trac.metadata.IValue | trac.metadata.ITagHeader[keyof trac.metadata.ITagHeader]): { value: ExtractedTracValue, basicType?: trac.BasicType, subBasicType?: trac.BasicType } => {

    // A lookup that helps us get the value from the value object
    const keyLookup: Record<trac.BasicType, keyof trac.metadata.IValue | undefined> = {
        [trac.BasicType.BASIC_TYPE_NOT_SET]: undefined,
        [trac.BOOLEAN]: "booleanValue",
        [trac.INTEGER]: "integerValue",
        [trac.FLOAT]: "floatValue",
        [trac.STRING]: "stringValue",
        [trac.DECIMAL]: "decimalValue",
        [trac.DATE]: "dateValue",
        [trac.DATETIME]: "datetimeValue",
        [trac.BasicType.ARRAY]: "arrayValue",
        [trac.BasicType.MAP]: "mapValue"
    }

    let value: ExtractedTracValue = null, basicType: undefined | trac.BasicType, subBasicType: undefined | trac.BasicType;

    // If the item is not an object assume that it is a simple key value pair from the header property
    if (!isObject(tracObject)) {

        value = typeof tracObject === "string" || typeof tracObject === "number" ? tracObject : null

        return {value: value}

    } else if (!isValue(tracObject)) {

        // If the item is not a trac.metadata.IValue object, and it is not a primitive value then it will be a trac.metadata.IDatetimeValue which is
        // how objectDatetime and tagDatetime properties are stored in the header
        value = tracObject.isoDatetime || null

        return {value: value}

    } else if (isValue(tracObject)) {

        // So now we know we have a trac Value object (trac.metadata.IValue), this is an object that has
        // a 'type' property. Some types need special processing to get the values such as arrays and dates
        basicType = tracObject?.type?.basicType == null ? undefined : tracObject?.type?.basicType

        // Not all TRAC values have a type descriptor in them (e.g. array items when adding attributes to job outputs)
        // So we have a bit of a hack in that we get the type by the key it does have that holds the value
        if (basicType == null) {

            if (tracObject.hasOwnProperty("stringValue")) {
                basicType = trac.STRING
            } else if (tracObject.hasOwnProperty("dateValue")) {
                basicType = trac.DATE
            } else if (tracObject.hasOwnProperty("datetimeValue")) {
                basicType = trac.DATETIME
            } else if (tracObject.hasOwnProperty("integerValue")) {
                basicType = trac.INTEGER
            } else if (tracObject.hasOwnProperty("booleanValue")) {
                basicType = trac.BOOLEAN
            } else if (tracObject.hasOwnProperty("decimalValue")) {
                basicType = trac.DECIMAL
            } else if (tracObject.hasOwnProperty("floatValue")) {
                basicType = trac.FLOAT
            } else {
                basicType = trac.BasicType.BASIC_TYPE_NOT_SET
            }
        }

        // For non-primitive types get the type inside the array or object
        if (basicType == trac.BasicType.ARRAY) {

            subBasicType = tracObject.type?.arrayType?.basicType == null ? undefined : tracObject.type?.arrayType?.basicType
            if (subBasicType == null) subBasicType = trac.BasicType.BASIC_TYPE_NOT_SET

        } else if (basicType == trac.BasicType.MAP) {

            // If the object has BASIC_TYPE_NOT_SET then the values are multiple types
            subBasicType = tracObject.type?.mapType?.basicType == null ? undefined : tracObject.type?.mapType?.basicType
            if (subBasicType == null) subBasicType = trac.BasicType.BASIC_TYPE_NOT_SET
        }

        if (basicType === trac.DATE) {

            value = tracObject.dateValue?.isoDate || null

        } else if (basicType === trac.DATETIME) {

            value = tracObject.datetimeValue?.isoDatetime || null

        } else if (basicType === trac.BasicType.ARRAY) {

            let tempArrayValue: (string | number | boolean)[] = []

            tracObject.arrayValue?.items && tracObject.arrayValue.items.forEach(item => {

                // THis requires the item to have a type descriptor in it (the value property)
                let temp = extractValueFromTracValueObject(item).value
                if (temp != null && isPrimitive(temp)) tempArrayValue.push(temp)
            })

            value = tempArrayValue

        } else if (basicType === trac.BasicType.MAP) {

            let tempMapValue: Record<string, boolean | number | string> = {}

            subBasicType = tracObject.type?.basicType || undefined

            tracObject.mapValue?.entries && Object.entries(tracObject.mapValue?.entries).forEach(([key, item]: [string, trac.metadata.IValue]) => {

                const newValue = extractValueFromTracValueObject(item)
                subBasicType = item.type?.basicType === null ? undefined : item.type?.basicType

                if (newValue) {
                    if (newValue.value !== null && isPrimitive(newValue.value)) tempMapValue[key] = newValue?.value
                }
            })

            value = tempMapValue

        } else if (basicType === trac.BasicType.DECIMAL) {

            value = tracObject.decimalValue?.decimal || null

        } else {

            // So now we get the actual value for basicTypes that do not require special processing
            const path = keyLookup[basicType]
            const tempValue = path ? tracObject[path] : null
            value = path && isPrimitive(tempValue) ? tempValue : null
        }

        return {basicType, subBasicType, value}

    } else {

        throw new TypeError("The TRAC value could not have its value extracted")
    }
}

/**
 * A function that takes a search result from the TRAC API or an object's metadata and returns its name to show the user.
 * This is based a hierarchy of what attributes are set for the object.
 *
 * @param tag - The tag for an object from the TRAC API.
 * @param showObjectId - Whether to add the object and tag version numbers into the name.
 * @param showVersions - Whether to add the object and tag version numbers into the name.
 * @param showCreatedDate - Whether to add in the created date to the label.
 * @param showUpdatedDate - Whether to add in the updated date to the label.
 */
export const getObjectName = (tag: trac.metadata.ITag, showObjectId: boolean = false, showVersions: boolean = false, showUpdatedDate: boolean = true, showCreatedDate: boolean = false): string => {

    // 'trac_job_output' is added by TRAC when a dataset is created from a job
    // it is the key that the dataset had in the flow, so if keys/names etc. are not added to the
    // output using the flow definition then we use this to tell the user which dataset a dataset is.
    const objectNameFromAttrs = tag.attrs?.hasOwnProperty("name") ? tag.attrs["name"].stringValue : tag.attrs?.hasOwnProperty("key") ? tag.attrs["key"].stringValue : tag.attrs?.hasOwnProperty("trac_job_output") ? tag.attrs["trac_job_output"].stringValue : undefined

    let objectName: string

    if (!objectNameFromAttrs) {

        // objectName will be "model", "flow" etc when there is no metadata attrs that can be used
        objectName = tag.header?.objectType ? convertObjectTypeToString(tag.header?.objectType, true, true) : "object"

    } else {

        objectName = objectNameFromAttrs
    }

    // Add in the object ID if requested or if there was no metadata attrs to help the user
    objectName = `${objectName}${!objectNameFromAttrs || showObjectId ? " " + tag.header?.objectId : ""}`

    let extraInfo = []

    if (showVersions) {
        extraInfo.push(`Object v${tag.header?.objectVersion} Tag v${tag.header?.tagVersion}`)
    }

    // The created timestamp for an object when accessed as an attribute is the
    // creation timestamp of version 1 of the object
    if (showCreatedDate && tag?.attrs?.trac_create_time?.datetimeValue?.isoDatetime != null) {
        extraInfo.push(`Created ${convertIsoDateStringToFormatCode(tag?.attrs?.trac_create_time?.datetimeValue?.isoDatetime, "DATETIME")}`)
    }

    if (showUpdatedDate && tag?.attrs?.trac_update_time?.datetimeValue?.isoDatetime != null) {
        extraInfo.push(`Updated ${convertIsoDateStringToFormatCode(tag?.attrs?.trac_update_time?.datetimeValue?.isoDatetime, "DATETIME")}`)
    }

    // job_tag is a UI owned metadata tag added to the outputs of a job, like "Stress scenario", we add it to the name because
    // user can use the outputs of jobs as the inputs of new jobs, we need a way to differentiate the outputs otherwise
    // the user is presented with a list of identically named default options
    let jobTag: null | string = null
    if (tag.header?.objectType === trac.ObjectType.DATA && tag?.attrs?.job_tag?.stringValue) {
        jobTag = tag.attrs.job_tag.stringValue
    }

    return `${objectName}${jobTag ? ` - ${jobTag}` : ""}${extraInfo.length > 0 ? ` (${extraInfo.join(", ")})` : ""}`
}

/**
 * A function that converts an array of metadata from an API search in TRAC to an array for use in the {@link SelectOption} component.
 * Note that the value in the option is a concatenation of the object ID and the object and tag version. This guarantees that
 * it is unique to the object.
 *
 * @remarks
 * This function processes whatever search results that it is passed as an argument, however the function that does the search
 * is limited in the number of results returned. This limit is set by maximumNumberOfOptionsReturned in the client-config.json.
 * Te reason for limiting the resulted number of results is performance.
 *
 * @param searchResults - The array of TRAC API metadata to convert.
 * @param showObjectId - Whether to add the object ID to the label.
 * @param showVersions - Whether to add in the object and tag versions to the label, used when searches
 * are done to return multiple versions of objects.
 * @param showUpdatedDate - Whether to add in the created date to the label.
 * @param showCreatedDate - Whether to add in the created date to the label.
 * @param includeTagVersion - Whether to add in the tag version to the unique key used for the value of the option.
 * @returns A set of options that can be used by the {@link SelectOption} component.
 */
export const convertSearchResultsIntoOptions = (searchResults: (trac.metadata.ITag | trac.metadata.Tag)[], showObjectId: boolean = false, showVersions: boolean = false, showUpdatedDate: boolean = true, showCreatedDate: boolean = false, includeTagVersion: boolean = true): Option<string, trac.metadata.ITag | trac.metadata.Tag>[] => {

    return searchResults.map(result => {

        if (result.header == null) throw new Error(`Search result in convertSearchResultsIntoOptions does not contain a header property`)

        return {
            value: createUniqueObjectKey(result.header, includeTagVersion),
            label: getObjectName(result, showObjectId, showVersions, showUpdatedDate, showCreatedDate),
            tag: result
        }
    })
}

/**
 * A function that creates a unique key from an object's header metadata. When includeTagVersion is true this
 * includes the object type, and the tag and object versions. This is what is used to specify a wholly unique
 * reference. When includeTagVersion is false the tag version is not included, the resulting key is equivalent
 * to the objectKey property used by the TRAC API.
 *
 * @param tagHeader - The header property of the TRAC object's metadata.
 * @param includeTagVersion - Whether to include the tagVersion in the unique key.
 *
 * @example
 * const header = {
 *              objectType: trac.ObjectType.FLOW,
 *              objectId: "597b589b-96fa-4e56-8066-d01fedcfcad1",
 *              objectVersion: 4,
 *              tagVersion: 5
 *              }
 * const key = createUniqueObjectKey(header, false)
 * console.log(key) // "FLOW-597b589b-96fa-4e56-8066-d01fedcfcad1-v4"
 *
 * @example
 * const header = {
 *              objectType: trac.ObjectType.DATA,
 *              objectId: "3327ff48-6311-4223-aa24-411f02656284",
 *              objectVersion: 1,
 *              tagVersion: 2
 *              }
 * const key = createUniqueObjectKey(header, true)
 * console.log(key) // "DATA-3327ff48-6311-4223-aa24-411f02656284-v1-v2"
 */
export const createUniqueObjectKey = (tagHeader: trac.metadata.ITagHeader, includeTagVersion: boolean = true): string => {

    const {objectId, objectType, objectVersion, tagVersion} = tagHeader

    if (objectType == null) {
        throw new Error("A tagHeader object has a missing property 'objectType' property")
    }

    return `${convertObjectTypeToString(objectType, false)}-${objectId}-v${objectVersion}${includeTagVersion ? `-v${tagVersion}` : ''}`
}

/**
 * A function that takes a property from an object's metadata header, for example the objectType property, extracts the value using the
 * Swiss army knife util we have and then enriches it. Enrichment is really just adding some more processing to make it more presentable
 * to the user. For example rather than showing the object type as an enum we convert it to a human-readable string.
 *
 * @param key - The key of the property being passed as the valueObject argument. There are rules for how to enrich the value based on
 * which one is passed.
 * @param valueObject - The header property to process.
 */
export const enrichMetadataHeader = (key: string, valueObject: trac.metadata.ITagHeader[keyof trac.metadata.ITagHeader]): undefined | { key: typeof key, value: ExtractedTracValue } => {

    let extractedValue = extractValueFromTracValueObject(valueObject)

    // This intercepts the key value pairs for the header part of the tag and converts the values to be more humanreadable
    if (key === "objectType" && isTracObjectType(extractedValue.value)) extractedValue.value = convertObjectTypeToString(extractedValue.value)
    if ((key === "objectTimestamp" || key === "tagTimestamp") && typeof extractedValue.value === "string") extractedValue.value = convertIsoDateStringToFormatCode(extractedValue.value, "DATETIME")

    return extractedValue ? {key, value: extractedValue.value} : undefined
}

/**
 * A function that takes a property from an object's metadata attributes, for example the businessSegments, extracts the value using the
 * Swiss army knife function we have and then enriches it. Enrichment is really just adding some more processing to make it more presentable
 * to the user. For example rather than showing the business segments as their raw keys we convert them to their labels. In general, we
 * can adjust the names of the properties and also apply formats to the values.
 *
 * Note that the processedAttributes argument comes from the processAttributes function and must have the right filtering
 * applied to the function for the type of trac object that the attribute is for. See the MetadataViewer component for
 * a demonstration. If you don't filter it to the right object type that the attribute being processed is for then you risk enriching it
 * with the wrong information.
 *
 * @param key - The key of the property being passed as the valueObject argument. There are rules for how to enrich the value based on
 * which one is passed.
 * @param valueObject - The TRAC value object for the attribute, containing its type and value information.
 * @param processedAttributes - The attributes for the type of TRAC object that the valueObject is for. See the
 * notes above for more information, this object is created by the processedAttributes function.
 */
export const enrichMetadataAttr = (key: string, valueObject: trac.metadata.IValue, processedAttributes: Record<string, UiAttributesProps>): { key: typeof key, value: ExtractedTracValue } => {

    // We are able to use attribute names as the key values so here is the variable we will use to store them. Showing
    // the name is better than trying to translate the key into a humanreadable string
    let finalKey = key

    // Is the attribute one generated automatically by TRAC
    let isTracAttribute: boolean = false

    let extractedValue = extractValueFromTracValueObject(valueObject)

    // Trac set attributes that are strings are converted to humanreadable strings by default
    // e.g. 'RUN_FLOW' -> 'Run flow'. However some attributes contain strings that you don't want to alter such as
    // error messages, anything in this array is not converted.
    const tracKeepAsRawString: string[] = ["trac_job_error_message", "trac_model_entry_point", "trac_model_path", "trac_model_package", "trac_model_package_group", "trac_model_repository"]

    if (extractedValue.value != null) {

        // Arrays and maps have basicType of ARRAY or MAP and a subBasicType that tells you what types are stored inside
        // the array or object properties
        const {subBasicType} = extractedValue

        // Use the name set for the attribute if one is set
        finalKey = (isKeyOf(processedAttributes, key) && processedAttributes[key].name && processedAttributes[key].name) || key

        // TRAC creates some attributes such as 'trac_create_time' that are pieces of the header tag but replicated in the
        // attr section to make them searchable - it adds 'trac_' to the start to make this obvious, here we remove the
        // 'trac_' part as the user won't really know what it means.
        if (finalKey.startsWith("trac_")) {

            isTracAttribute = true
            finalKey = finalKey.substring(5)
        }

        // This function enables the formatting of attributes, we intercept when the user is showing the table of attributes
        // Then look them up in the attribute data to see if there is any format to apply

        // If the basicType of the variable or its subType for maps and arrays can have a format applied
        if (hasOwnProperty(processedAttributes, key) && isPrimitive(extractedValue.value) && isTracFormatable(extractedValue.basicType)) {

            // First if the value is a string, number or boolean (primitives)
            extractedValue.value = setAnyFormat(extractedValue.basicType, processedAttributes[key].formatCode, extractedValue.value)

        } else if (hasOwnProperty(processedAttributes, key) && Array.isArray(extractedValue.value) && subBasicType !== undefined) {

            // Second if the value is an array of strings, numbers or booleans

            // Now at this stage we can try and enrich array values set by options and get the option label rather than the value.
            if (processedAttributes[key].useOptions) {

                // Extract the options from the definition
                const {options} = processedAttributes[key]

                // If the options exist
                if (options !== undefined) {

                    // Go through each extracted value from the array in the attribute
                    extractedValue.value = extractedValue.value.map(item => {

                        // Work out if the defined options are in a groups or just a straight list
                        if (isGroupOption(options)) {

                            // See if the value we are trying to enrich is in the groups of options
                            const {tier, index} = getGroupOptionIndex({value: item, label: item.toString()}, options)

                            // If it exists then return the label
                            if (tier > -1 && index > -1) {
                                return options[tier].options[index]?.label ?? item
                            }

                        } else {

                            // For a straight forward list of options we find the option and return the label
                            return options.find(option => option.value === item)?.label ?? item
                        }

                        return item

                    }).filter(isDefined)
                }

            } else if (isTracFormatable(extractedValue.subBasicType)) {
                // Numbers and dates can have a format applied
                extractedValue.value = extractedValue.value.map(item => setAnyFormat(subBasicType, processedAttributes[key].formatCode, item)).filter(isDefined)
            }

        } else if (isObject(extractedValue.value) && subBasicType && isTracFormatable(extractedValue.subBasicType)) {

            // Third if the value is an object of strings, numbers or booleans
            Object.entries(extractedValue.value).forEach(([key, item]) => {
                if (extractedValue && extractedValue.value !== null && hasOwnProperty(extractedValue.value, key)) extractedValue.value[key] = setAnyFormat(subBasicType, processedAttributes[key].formatCode, item)
            })
        }
    }

    // Attributes are added automatically added by TRAC, if these are strings we format then so 'RUN_FLOW'
    // becomes 'Run flow' - as long as they are not object IDs or keys or a GitHub commit hash
    return {
        key: finalKey,
        value: isTracAttribute && !tracKeepAsRawString.includes(key) && extractedValue.basicType === trac.STRING && extractedValue.value && !isObjectId(extractedValue.value) && !isObjectKey(extractedValue.value) && key !== "trac_model_version" ? convertKeyToText(extractedValue.value.toString()) : extractedValue.value
    }
}

/**
 * WE HAVE GOT TO HERE JEST
 */

export const enrichMetadataAndSetDefault = (attribute: undefined | null | trac.metadata.IValue, processedAttributes: Record<string, UiAttributesProps>, defaultValue: string = "Not set") => {

    return attribute ? enrichMetadataAttr("not used", attribute, processedAttributes)?.value || {value: defaultValue} : defaultValue
}

/**
 * A function that takes an object's TRAC metadata tag and extracts the values listed in the first 'propertiesToShow' argument. The UI dataset with all the
 * attribute definitions is passed in and used to augment the values. For example...
 *
 * @remarks Examples of enrichment:
 * Object types in the header stored as enums are converted to string versions.
 * Datetime values in the header (e.g. object creation datetime) are converted into a datetime format.
 * TRAC owned attributes in the attr property have the 'trac_' part removed from the key.
 * Attributes in the attr property are converted to humanreadable strings unless in an exclusion list.
 * Attributes in the attr property have their keys changed to be the name of the attribute as defined in the UI (if set).
 * Attributes in the attr property have a format applied if one is defined in the UI.
 * Attributes in the attr property have option values translated into options labels.
 *
 * @param propertiesToShow - The metadata attributes to extract and enrich. The attribute name is in the property while the tag is where the attribute is stored.
 * For example the objectId would be in the header. The attribute can also be nested inside the definition in the metadata, if this is the case then the array is
 * the path to attribute. This path approach is used when accessing the attributes added to the models loaded via an import model job, this path must point to
 * an array of attributes and all the attributes will be added to the final information returned.
 * @param metadata - The TRAC metadata to get the attribute values from.
 * @param allProcessedAttributes - The attributes as defined by the user interface. This is owned by the {@link applicationSetupStore} store.
 */
export function enrichProperties(propertiesToShow: { tag: "attrs" | "header" | string[], property: string }[], metadata: trac.metadata.ITag, allProcessedAttributes: Record<string, UiAttributesProps>): { key: string, value: DataValues | (DataValues)[] | { [key: string]: DataValues } }[] {

    const objectType = metadata.header?.objectType ?? trac.ObjectType.OBJECT_TYPE_NOT_SET

    // If there is an object type defined go and get all the attributes for that object type, otherwise set an
    // empty object which will mean that no additional formatting will be done. So if the metadata is for a dataset this
    // gets all the user interface defined attributes for datasets.
    const processedAttributesForObject = getAttributesByObject(allProcessedAttributes, [convertObjectTypeToString(objectType, false)])

    const {attrs, header} = metadata

    // This is the final information we are going to return with the enriched data
    let finalTable: (undefined | { key: string, value: DataValues | (DataValues)[] | { [key: string]: DataValues } })[] = []

    // Note we iterate over the original list of items
    propertiesToShow.forEach(tableRow => {

        const {property, tag} = tableRow

        // This intercepts the key value pairs in the TRAC API metadata object for the
        // header and converts the values to be more humanreadable
        if (tag === "header" && header && hasOwnProperty(header, property)) {

            // Header properties are primitive types or TRAC DatetimeValues
            const item = header[property]

            if (isPrimitive(item) || isDatetimeValue(item)) {
                finalTable.push(enrichMetadataHeader(property, item))
            }
        }
        // This intercepts the key value pairs in the TRAC API metadata object for the attrs and converts the values to be more humanreadable
        else if (tag === "attrs" && attrs && hasOwnProperty(attrs, property)) {

            finalTable.push(enrichMetadataAttr(property, attrs[property], processedAttributesForObject))
        }
        // This is for deeply nested properties in the metadata
        else if (Array.isArray(tag)) {

            // Get the nested property
            const nestedProperty = getNestedObject(metadata, [...tag, property])

            // In TRAC any deeply nested item that is an array is typically an array of tag updates, these
            // can be added as rows to the table and enriched
            if (Array.isArray(nestedProperty)) {

                nestedProperty.forEach((item: any) => {

                    if (isITagUpdate(item) && typeof item.attrName === "string" && isValue(item.value)) {

                        // Note that we don't know what the type of object that the attributes at the nested
                        // location are for, so we pass down the unfiltered list, this contains ALL the
                        // UI defined attributes - there is a risk that say the attributes are for a dataset
                        // created by a job that we enrich the attribute with incorrect information.
                        finalTable.push(enrichMetadataAttr(item.attrName, item.value, allProcessedAttributes))
                    }
                })

            } else if (isPrimitive(nestedProperty) || isValue(nestedProperty) || isDatetimeValue(nestedProperty)) {

                // If not an array we assume we can extract the information as standard
                finalTable.push({key: property, value: extractValueFromTracValueObject(nestedProperty).value})
            }
        }
        // If new entry points are opened up then this can be used to get the right data
        //else return undefined
    })

    return finalTable.filter(isDefined)
}

/**
 * A function that takes an object's metadata tag and extracts the values into a format that can be passed to a Select component,
 * for example if the attribute is for an array of string values then these are converted to a set of options for the
 * SelectOption component. This is used when editing an object's tags in the {@link UpdateTagsScene} for example.
 *
 * @param tag - The TRAC metadata Tag.
 * @param processedAttributesForObject - The attributes dataset converted by the processAttributes function.
 * TODO this needs to have an option to add in attributes/parameters from the tags if they are not defined in the options
 *  this is for parameters in re-run jobs where the parameter is an option and we need to ensure the original options are all copied across
 */
export const convertTagAttributesToSelectValues = (tag: trac.metadata.ITag, processedAttributesForObject: Record<string, UiAttributesProps>): Record<string, DeepWritable<SelectPayload<Option, boolean>["value"]>> => {

    // The extracted values that we are going to return
    let values: Record<string, DeepWritable<SelectPayload<Option, boolean>["value"]>> = {}

    // Go through each allowed attribute for the object type
    Object.keys(processedAttributesForObject).forEach((key => {

        // If the tag has an attribute that is in the master list we can get the value from the
        // tag and use that as the value, however we need to ensure that the type is right, someone
        // might change the attribute type but keep the name the same
        if (hasOwnProperty(tag.attrs, key)) {

            // Get the value from the attribute an array will returned as ["A", "B"] for example
            const value = extractValueFromTracValueObject(tag.attrs[key])

            // If the basic type of the attribute in the tag is still the same as the one in the definition
            // then we are good to put it in the store. This is only valid for primitive types (null, string
            // booleans and numbers), array and object types need to be dealt with differently
            if (value.basicType === processedAttributesForObject[key].basicType && isPrimitive(value.value)) {

                // This needs to get the right types of values for the attributes from the attributes data
                values[key] = value.value

            } else if (value.basicType === trac.metadata.BasicType.ARRAY) {

                // This is the options to set for the attribute
                let selectedOptions: Option[] = []

                Array.isArray(value.value) && value.value.forEach((item) => {

                    // The options defined for the attribute
                    const options = processedAttributesForObject[key].options || []

                    // If the options are grouped (business segments are set as grouped options with headers) then
                    // extracting the object has to be done differently to a simple set of options
                    if (isGroupOption(options)) {

                        // See if the value in the tag exists as an option
                        const {tier, index} = getGroupOptionIndex({
                            value: item,
                            label: (item || "Not set").toString()
                        }, options)

                        // If the option exists then add it to the list
                        if (tier > -1 && index > -1) {

                            const optionToAdd = options[tier].options[index]

                            if (optionToAdd && isStringOption(optionToAdd) && isTracString(value.subBasicType)) {
                                selectedOptions.push(optionToAdd)
                            } else if (optionToAdd && isIntegerOption(optionToAdd) && value.subBasicType === trac.INTEGER) {
                                selectedOptions.push(optionToAdd)
                            } else if (optionToAdd && isNumberOption(optionToAdd) && isTracNumber(value.subBasicType)) {
                                selectedOptions.push(optionToAdd)
                            } else if (optionToAdd && isBooleanOption(optionToAdd) && isTracBoolean(value.subBasicType)) {
                                selectedOptions.push(optionToAdd)
                            } else if (optionToAdd && isDateOption(optionToAdd) && value.subBasicType === trac.DATE) {
                                selectedOptions.push(optionToAdd)
                            } else if (optionToAdd && isDatetimeOption(optionToAdd) && value.subBasicType === trac.DATETIME) {
                                selectedOptions.push(optionToAdd)
                            }
                        }

                    } else {

                        // Simple lists of options are easier
                        const optionToAdd = options.find(option => option.value === item)
                        if (optionToAdd) selectedOptions.push(optionToAdd)
                    }
                })

                // A multi value SelectOption component has a different value type compared to single value SelectOption
                // version
                values[key] = processedAttributesForObject[key].isMulti ? selectedOptions : selectedOptions.length > 0 ? selectedOptions[0] : null

            } else {

                values[key] = null
                values[key] = null
            }

        } else {
            values[key] = null
            values[key] = null
        }
    }))

    return values
}

/**
 * A function that takes a metadata tag for an object and checks if the items listed in the propertiesToShow argument
 * are available. This is done for two reasons. First, in case the number of found properties is zero we can show a
 * message to the user. Second, it avoids lots of complex clauses in the render.
 *
 * @param propertiesToShow - The lists of properties to show keyed by the property that they are stored in.
 * This allows for properties with the same name but different values in the header and attrs to be selected.
 * @param metadata - The TRAC metadata to show in the table.
 * @returns The elements of propertiesToShow that exist in the provided metadata tag.
 */
export function checkProperties(propertiesToShow: { tag: "attrs" | "header" | string[], property: string }[], metadata: trac.metadata.ITag): { tag: "attrs" | "header" | string[], property: string }[] {

    return propertiesToShow.filter(item => {

        // If the tag is a string ( "attrs" and "header" allowed)
        if (!Array.isArray(item.tag)) {

            // If the metadata has the required tag return the object from the metadata,
            // return the original property
            return Boolean(hasOwnProperty(metadata, item.tag) && metadata[item.tag])

        } else {

            // If the tag is an array of strings, get the nested object by path
            return Boolean(getNestedObject(metadata, [...item.tag, item.property]))
        }
    })
}

export function sortOptionsByObjectTimeStamp(a: trac.metadata.ITag, b: trac.metadata.ITag): number {

    if (!a.header?.objectTimestamp?.isoDatetime || !b.header?.objectTimestamp?.isoDatetime) return 0
    if (a.header?.objectTimestamp?.isoDatetime < b.header?.objectTimestamp?.isoDatetime) {
        return 1;
    }
    if (a.header?.objectTimestamp?.isoDatetime > b.header?.objectTimestamp?.isoDatetime) {
        return -1;
    }
    return 0;
}

/**
 * A function that sets the label to show for a variable in a dataset. The fieldLabel property in a TRAC schema
 * is optional and when not set the fieldName is used. There is an option to always include the variable name
 * in the label, even if a label is set.
 *
 * @param variable - A variable from a schema in TRAC.
 * @param labelIncludesName - Whether to add in the fieldName even if there is a label set.
 */
export const setLabel = (variable: trac.metadata.IFieldSchema, labelIncludesName: boolean): string => {

    const {fieldName, label} = variable

    const nameEqualsLabel = Boolean(fieldName === label)

    return label != null && label !== "" ? `${label}${labelIncludesName && !nameEqualsLabel ? ` (${fieldName})` : ""}` : fieldName || "Unknown"
}

/**
 * A function that takes URL parameters from the React Router API and converts them into a valid tag selector.
 * This is used where the user or the application adds parameters to the URL, and we need to search for the corresponding item.
 */
export const getTagSelectorFromUrlParameters = ({
                                                    objectType,
                                                    objectId,
                                                    objectVersion,
                                                    tagVersion,
                                                    searchAsOf
                                                }: { objectType: trac.ObjectType, objectId: string | undefined, objectVersion: string | undefined, tagVersion: string | undefined, searchAsOf: null | trac.metadata.IDatetimeValue }): { tagSelector: undefined | trac.metadata.ITagSelector, errorMessages: string[] } => {

    // This is the tag selector that we are going to build
    let tagSelector: trac.metadata.ITagSelector = {
        objectType,
        objectId: undefined,
        objectVersion: undefined,
        tagVersion: undefined,
        objectAsOf: searchAsOf,
        tagAsOf: searchAsOf,
        latestObject: undefined,
        latestTag: undefined
    }

    // This is an array of error messages that we can use to tell the user the tag selector could not be built.
    const errorMessages: string[] = []

    // If the object ID is not valid then show a message
    if (isObjectId(objectId)) {
        tagSelector.objectId = objectId
    } else {
        errorMessages.push("The ID is not a valid object ID.")
    }

    // The URL parameters for object ang tag versions will be integers passed as strings or 'latest' or undefined (not set).
    if (typeof objectVersion === "string" && isValidNumberAsString(objectVersion)) {

        const objectVersionAsNumber = convertStringToInteger(objectVersion)

        if (objectVersionAsNumber.toString() === objectVersion) {
            tagSelector.objectVersion = objectVersionAsNumber
        } else {
            errorMessages.push("The object version is not valid, it can be either an integer or 'latest'.")
        }

    } else if (objectVersion === undefined || objectVersion?.toLowerCase() === "latest") {
        tagSelector.latestObject = true
    } else {
        errorMessages.push("The object version is not valid, it can be either an integer or 'latest'.")
    }

    if (typeof tagVersion === "string" && isValidNumberAsString(tagVersion) ) {

        const tagVersionAsNumber = convertStringToInteger(tagVersion)

        if (tagVersionAsNumber.toString() === tagVersion) {
            tagSelector.tagVersion = convertStringToInteger(tagVersion)
        } else {
            errorMessages.push("The tag version is not valid, it can be either an integer or 'latest'.")
        }

    } else if (tagVersion === undefined || tagVersion?.toLowerCase() === "latest") {
        tagSelector.latestTag = true
    } else {
        errorMessages.push("The tag version is not valid, it can be either an integer or 'latest'.")
    }

    return {errorMessages, tagSelector}
}
