/**
 * A group of utilities that primarily deal with processing a set of metadata attributes or model parameters stored in a
 * dataset downloaded from TRAC by converting them into an object that can be used as props to the {@link ParameterMenu}
 * @category Utils
 * @module AttributeAndParameterUtils
 */

import {arraysOfPrimitiveValuesEqual, sortArrayBy} from "./utils_arrays";
import type {DataValues, DeepWritable, ExtractedTracValue, GenericOption, MetadataTableToShow, Option, SelectPayload, UiBusinessSegmentsDataRow} from "../../types/types_general";
import type {UiAttributesListRow, UiAttributesObject1, UiAttributesObject2, UiAttributesObject3, UiAttributesProps} from "../../types/types_attributes_and_parameters";
import {
    convertBusinessSegmentDataToOptions,
    convertDataBetweenTracTypes,
    createDateRangeOptions,
    createNumberRangeOptions,
    createOptionsFromConcatenatedStrings,
    getDateFromStringInstruction,
    setTracValue
} from "./utils_general";
import {convertIsoDateStringToFormatCode, isValidNumberFormatCode} from "./utils_formats";
import {convertObjectTypeToString, enrichMetadataAttr, enrichMetadataHeader, extractValueFromTracValueObject} from "./utils_trac_metadata";
import {convertStringToObjectType, isValidIsoDateString, isValidIsoDatetimeString, isValidNumberAsString} from "./utils_string";
import {
    hasOwnProperty,
    isDateFormat,
    isDefined,
    isGroupOption,
    isMultiOption,
    isOption,
    isPrimitive,
    isTracBoolean,
    isTracDateOrDatetime,
    isTracNumber,
    isTracObjectTypeString,
    isTracString
} from "./utils_trac_type_chckers";
import type {MultiValue, SingleValue} from "react-select";
import {roundNumberToNDecimalPlaces} from "./utils_number";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * A function that converts the attributes or parameter dataset stored in TRAC to an object of attributes/parameters
 * keyed by their ID and with all the default, minimum, maximum and options set up so that they can be used as props
 * in the ParameterMenu component. This is the master function that calls a set of other functions.
 *
 * @param data - The attributes or parameter dataset to process.
 * @param businessSegmentsData - The UI dataset for the business segment definitions, this is downloaded
 * and stored in the {@link applicationSetupStore} and is used to get the options for the business segment attribute.
 * @param businessSegmentsSchema - The schema for the UI dataset for the business segment definitions, this is needed
 * because the user is able to edit the labels for the business segments, so these names are needed to define the groups
 * in the business segment attribute options.
 * @param objectTypes - The array of TRAC object types to filter the attributes by e.g. [FLOW, MODEL]
 * @param disabled - The array of attributes that should be disabled from the user editing them, this is
 * usually as the application will provide the value.
 */
export const processAttributes = (data: UiAttributesListRow[], businessSegmentsData: UiBusinessSegmentsDataRow[], businessSegmentsSchema: trac.metadata.IFieldSchema[], objectTypes?: string[], disabled?: string[]): Record<string, UiAttributesProps> => {

    return setDefaultParameterValues(addAttributeMinimumAndMaximums(addAttributeOptions(mapAttributesDataToObject(data, objectTypes, disabled), businessSegmentsData, businessSegmentsSchema)))
}

/**
 * A function that takes the full processed attributes and filters this down to just those that are for a specific
 * set of object types.
 *
 * @param allProcessedAttributes - The full set of attribute definitions for all TRAC object types. This is the output of the {@link processAttributes}
 * function.
 * @param objectTypes - An array of TRAC object types, only attributes defied for these object types will be returned. Note that the array contains the
 * string name not the enum e.g. '["DATA"]'.
 */
export const getAttributesByObject = (allProcessedAttributes: Record<string, UiAttributesProps>, objectTypes: string[]): Record<string, UiAttributesProps> => {

    let attributesForObject: Record<string, UiAttributesProps> = {}

    // If someone uses %object% in an attribute's name, description or tooltip then we replace this with the type of the
    // object so "%object%" becomes "dataset". We do this so that the test is more specific. We can only do this is we
    // are being asked to select a single object type of attributes.
    let replacementString: string = "object"

    // If there is only one object type being extracted from the full set of attributes then set a string that
    // will replace %object%
    if (objectTypes.length === 1 && isTracObjectTypeString(objectTypes[0])) {

        const objectTypeAsInteger = convertStringToObjectType(objectTypes[0])
        // We have to go back through to a string in order to have the right case and name
        replacementString = convertObjectTypeToString(objectTypeAsInteger, true, true)
    }

    Object.keys(allProcessedAttributes).forEach(key => {

        if (allProcessedAttributes[key].objectTypes.includes("ALL") || allProcessedAttributes[key].objectTypes.some(object => objectTypes.includes(object))) {

            attributesForObject[key] = {...allProcessedAttributes[key]}

            // Make the text string more intuitive for the object type
            if (attributesForObject[key].name) {
                attributesForObject[key].name = (attributesForObject[key].name || "").replace(/%object%/gi, replacementString)
            }
            if (attributesForObject[key].description) {
                attributesForObject[key].description = (attributesForObject[key].description || "").replace(/%object%/gi, replacementString)
            }
            if (attributesForObject[key].tooltip) {
                attributesForObject[key].tooltip = (attributesForObject[key].tooltip || "").replace(/%object%/gi, replacementString)
            }
        }
    })

    return attributesForObject
}

/**
 * A function that takes the attributes or parameter list datasets loaded from TRAC in the applicationSetupStore
 * and converts them to an object keyed to the ID value. The output of this then has the convertAttributeStringsToProps
 * function applied to it to convert the string values into props for the ParameterMenu component.
 * This is used by multiple scenes where attributes are set by the user.
 *
 * @remarks
 * This is the first processing step of the {@link processAttributes} function.
 *
 * @param data - The attributes or parameter dataset to process.
 * @param objectTypes - The array of TRAC object types to filter the attributes by e.g. [FLOW, MODEL]
 * @param disabled - The array of attributes that should be disabled from the user editing them, this is
 * usually as the application will provide the value
 */
export const mapAttributesDataToObject = (data: UiAttributesListRow[], objectTypes?: string[], disabled?: string[]): Record<string, UiAttributesObject1> => {

    let dataAsObject: Record<string, UiAttributesObject1> = {}

    data.forEach((attribute) => {

        const objectTypesArray = attribute.OBJECT_TYPES ? attribute.OBJECT_TYPES.split("||") : []

        // Filter out attributes and parameters that are not in the required list, if no list is provided then return them all
        if (attribute.ID != null && attribute.BASIC_TYPE != null && attribute.OBJECT_TYPES != null && (!objectTypes || objectTypes.length === 0 || objectTypes.includes("ALL") || objectTypesArray.includes("ALL") || objectTypes.some(r => objectTypesArray.includes(r)))) {

            dataAsObject[attribute.ID] = {

                // We map each row of data to a new object for the store, since undefined is not allowed in the data
                // we convert some to 'undefined' to make using them in the components more straight forward (some
                // components accept undefined but not null values, and we want to avoid 'if value === null ? undefined : value)'

                // objectTypes is not the object enum (e.g. 1, 2) but the string version (e.g. DATA, FLOW)
                objectTypes: attribute.OBJECT_TYPES.split("||"),
                id: attribute.ID,
                basicType: attribute.BASIC_TYPE,
                name: attribute.NAME,
                description: attribute.DESCRIPTION,
                category: attribute.CATEGORY || "GENERAL",
                minimumValue: attribute.MINIMUM_VALUE,
                maximumValue: attribute.MAXIMUM_VALUE,
                optionsIncrement: attribute.OPTIONS_INCREMENT,
                // We map null to undefined in order to prevent having to do this during the render phase
                tooltip: !attribute.TOOLTIP ? undefined : attribute.TOOLTIP,
                // Hide any attributes that are set by the UI
                hidden: attribute.HIDDEN === null && attribute.SET_AUTOMATICALLY_BY_APPLICATION === null ? false : Boolean(attribute.HIDDEN || attribute.SET_AUTOMATICALLY_BY_APPLICATION),
                linkedToId: attribute.LINKED_TO_ID,
                linkedToValue: attribute.LINKED_TO_VALUE,
                isMulti: attribute.IS_MULTI === null ? false : attribute.IS_MULTI,
                defaultValue: attribute.DEFAULT_VALUE,
                formatCode: attribute.FORMAT_CODE,
                optionValues: attribute.OPTIONS_VALUES,
                optionLabels: attribute.OPTIONS_LABELS,
                // We map null to undefined in order to prevent having to do this during the render phase
                mustValidate: attribute.MUST_VALIDATE === null ? false : attribute.MUST_VALIDATE,
                useOptions: attribute.USE_OPTIONS === null ? false : attribute.USE_OPTIONS,
                disabled: Boolean(disabled && disabled.includes(attribute.ID)),
                widthMultiplier: attribute.WIDTH_MULTIPLIER,
                numberOfRows: attribute.NUMBER_OF_ROWS,
                options: undefined
            }
        }
    })

    return dataAsObject
}

/**
 * A function that takes the outputs of the {@link mapAttributesDataToObject} function and adds in the relevant options for each
 * attribute/parameter if it is set to use options, these can then be used in a {@link SelectOption} component. It also sets
 * string attributes to a text area if they have a number of rows set.
 *
 * @remarks
 * This is the second processing step of the {@link processAttributes} function.
 *
 * @param dataAsObject - The attribute/parameter object from the mapAttributesDataToObject function.
 * @param businessSegmentsData - The UI dataset for the business segment definitions, this is downloaded
 * and stored in the {@link applicationSetupStore} and is used to get the options for the business segment attribute.
 * @param businessSegmentsSchema - The schema for the UI dataset for the business segment definitions, this is needed
 * because the user is able to edit the labels for the business segments, so these names are needed to define the groups
 * in the business segment attribute options.
 */
export const addAttributeOptions = (dataAsObject: Record<string, UiAttributesObject1>, businessSegmentsData: UiBusinessSegmentsDataRow[], businessSegmentsSchema: trac.metadata.IFieldSchema[]): Record<string, UiAttributesObject2> => {

    let dataWithOptions: Record<string, UiAttributesObject2> = {}

    // Now what we are going to do it a bit of skulduggery. The business segments we assign an object in TRAC is just
    // attribute, although it is one that the user can not edit. However, unlike other attributes that are un-editable
    // the application has a separate dataset that is used to define the options for the business segments (we do this
    // as the structure has a hierarchy). So we pass the business segment dataset in as an argument here and intercept
    // the attribute being processed - then we splice in the options.

    const businessSegmentOptions = convertBusinessSegmentDataToOptions(businessSegmentsData, businessSegmentsSchema)

    // Parse each parameter in the object and parse the string values into parameter metadata
    Object.entries(dataAsObject).forEach(([key, oldAttribute]) => {

        // Copy the object to prevent mutation of the argument.
        let attribute: UiAttributesObject2 = {...oldAttribute}

        if (attribute.basicType == null) throw new TypeError(`The attribute ${attribute.id} has a null basicType set, this not allowed`)

        // If the parameter has minimum and a maximum value and an increment then create a range
        if (attribute.useOptions && attribute.minimumValue != null && attribute.maximumValue != null && attribute.optionsIncrement != null) {

            if (isTracDateOrDatetime(attribute.basicType) && isDateFormat(attribute.optionsIncrement) && isDateFormat(attribute.formatCode)) {

                attribute.options = createDateRangeOptions(attribute.minimumValue, attribute.maximumValue, attribute.optionsIncrement, attribute.formatCode)
                attribute.specialType = "OPTION"

            } else if (isTracNumber(attribute.basicType) && isValidNumberAsString(attribute.minimumValue) && isValidNumberAsString(attribute.maximumValue) && isValidNumberAsString(attribute.optionsIncrement) && isValidNumberFormatCode(attribute.formatCode)) {

                attribute.options = createNumberRangeOptions(parseFloat(attribute.minimumValue), parseFloat(attribute.maximumValue), parseFloat(attribute.optionsIncrement), attribute.basicType, attribute.formatCode)
                attribute.specialType = "OPTION"
            }

            attribute.minimumValue = null
            attribute.maximumValue = null
            attribute.optionsIncrement = null

            // If the parameter has at least one option values set then use these to generate options
        } else if (attribute.useOptions && attribute.optionValues) {

            attribute.options = createOptionsFromConcatenatedStrings(attribute.optionValues, attribute.optionLabels, attribute.basicType, attribute.formatCode)

            // Skullduggery here
            if (key === "business_segments") attribute.options = businessSegmentOptions
            attribute.specialType = "OPTION"
            attribute.optionValues = null
            attribute.optionLabels = null

            // If the parameter is a string with a valid numeric format code show it as a text area
        } else if (attribute.basicType === trac.STRING && attribute.numberOfRows > 1) {
            attribute.specialType = "TEXTAREA"
        }

        // Add the updated object to the new object
        dataWithOptions[key] = attribute
    })

    return dataWithOptions
}


/**
 * A function that takes the outputs of the {@link addAttributeOptions} function and adds in the relevant default
 * minimum and maximum values for each attribute/parameter, these can then be used as the value in the Select components.
 *
 * @remarks
 * This is the third processing step of the {@link processAttributes} function.
 *
 * @param dataAsObject - The attribute/parameter object from the mapAttributesDataToObject function.
 */
export const addAttributeMinimumAndMaximums = (dataAsObject: Record<string, UiAttributesObject2>): Record<string, UiAttributesObject3> => {

    let objectWithMinMax: Record<string, UiAttributesObject3> = {}

    // Parse each parameter in the object and set its minimum and maximum values
    Object.entries(dataAsObject).forEach(([key, oldAttribute]) => {

        let attribute: UiAttributesObject3 = {...oldAttribute}

        if (isTracDateOrDatetime(attribute.basicType)) {

            // We use the old attribute because of typescript, this function changes the type of these variables
            attribute.minimumValue = getDateFromStringInstruction(oldAttribute.minimumValue, "dateIso")
            attribute.maximumValue = getDateFromStringInstruction(oldAttribute.maximumValue, "dateIso")

        } else if (isTracNumber(attribute.basicType)) {

            attribute.minimumValue = convertDataBetweenTracTypes(trac.STRING, attribute.basicType, oldAttribute.minimumValue)
            attribute.maximumValue = convertDataBetweenTracTypes(trac.STRING, attribute.basicType, oldAttribute.maximumValue)

        } else if (attribute.basicType === trac.STRING) {

            attribute.minimumValue = convertDataBetweenTracTypes(trac.STRING, trac.INTEGER, oldAttribute.minimumValue)
            attribute.maximumValue = convertDataBetweenTracTypes(trac.STRING, trac.INTEGER, oldAttribute.maximumValue)

        } else {

            attribute.minimumValue = null
            attribute.maximumValue = null
        }

        objectWithMinMax[key] = attribute
    })

    return objectWithMinMax
}

/**
 * A function that takes the outputs of the {@link addAttributeMinimumAndMaximums} function and adds in the relevant default
 * values for each attribute/parameter, these can then be used as the value in the Select components.
 *
 * @remarks
 * This is the fourth processing step of the {@link processAttributes} function.
 *
 * @param dataAsObject - The attribute/parameter object from the mapAttributesDataToObject function.
 */
export const setDefaultParameterValues = (dataAsObject: Record<string, UiAttributesObject3>): Record<string, UiAttributesProps> => {

    let objectWithDefaults: Record<string, UiAttributesProps> = {}

    // Parse each parameter in the object and set its initial value
    Object.entries(dataAsObject).forEach(([key, oldAttribute]) => {

        // See https://stackoverflow.com/questions/34698905/how-can-i-clone-a-javascript-object-except-for-one-key
        // This removes some the properties while copying the rest of the object, we do this to lean up the final
        // object after all the processing
        let attribute: UiAttributesProps = (({optionsIncrement, optionLabels, optionValues, ...others}) => others)(oldAttribute)

        if (!attribute.defaultValue) {

            attribute.defaultValue = null

        } else if (attribute.basicType === trac.DATE) {

            // We use the old attribute because of typescript, this function changes the type of this variable
            attribute.defaultValue = getDateFromStringInstruction(oldAttribute.defaultValue, "dateIso")

        } else if (attribute.basicType === trac.DATETIME) {

            // We use the old attribute because of typescript, this function changes the type of this variable
            attribute.defaultValue = getDateFromStringInstruction(oldAttribute.defaultValue, "datetimeIso")

        } else if (attribute.basicType != null) {

            // We use the old attribute because of typescript, this function changes the type of this variable
            // @ts-ignore
            attribute.defaultValue = convertDataBetweenTracTypes(trac.STRING, attribute.basicType, oldAttribute.defaultValue)

        } else {

            attribute.defaultValue = null
        }

        // If the attribute or parameter is actually an option then try and find that option and set it as the default
        if (attribute.useOptions && attribute.options && attribute.defaultValue !== null) {

            // Need to get the search options from a group as well as just a vanilla list
            const actualOptions = (hasOwnProperty(attribute.options[0], "options") ? attribute.options[0]?.options : attribute.options) || []

            // @ts-ignore
            attribute.defaultValue = actualOptions.find(option => option.value === attribute.defaultValue) || null
        }
            // This is for when the user says that an attribute/parameter is multivalued but does not use options. In this
        // case we will use the SelectOption component, but we set the component as creatable.
        else if (!attribute.useOptions && attribute.isMulti && typeof attribute.defaultValue === "string" && attribute.basicType != null) {
            // @ts-ignore
            attribute.defaultValue = createOptionsFromConcatenatedStrings(attribute.defaultValue, attribute.defaultValue, attribute.basicType, attribute.formatCode)
        }


        // Add the updated object to the new object
        objectWithDefaults[key] = attribute
    })

    return objectWithDefaults
}

/**
 * A function that takes the final processed attribute/parameter object and groups their keys by their category. This means
 * that when showing the attributes/parameters in the application in by category we know which is in each.
 *
 * @param dataAsObject - The attribute/parameter object from the processAttributes function.
 */
export const setParametersByCategory = (dataAsObject: Record<string, UiAttributesProps>): Record<string, string[]> => {

    let parametersByCategory: Record<string, string[]> = {}

    Object.values(dataAsObject).forEach(parameter => {

        const category = parameter.category.toUpperCase()

        if (!parametersByCategory.hasOwnProperty(category)) {
            parametersByCategory[category] = []
        }

        parametersByCategory[category].push(parameter.id)
    })

    return parametersByCategory
}

/**
 * A function that takes the final processed attribute/parameter object and then extracts the default values into a
 * separate object as if they were from the payloads of the Select components. This object can then be placed into a
 * store as the initial value of each attribute/parameter.
 *
 * @param dataAsObject - The attribute/parameter object from the processAttributes function.
 */
export const extractDefaultValues = (dataAsObject: Record<string, UiAttributesProps>): Record<string, DeepWritable<SelectPayload<Option, boolean>["value"]>> => {

    const defaultValues: Record<string, DeepWritable<SelectPayload<Option, boolean>["value"]>> = {}

    Object.values(dataAsObject).forEach(attribute => {

        defaultValues[attribute.id] = attribute.defaultValue
    })

    return defaultValues
}

/**
 * A function that extracts the information in a particular key in a TRAC metadata tag and flattens this into a simple
 * array of objects containing the extracted key and value. It also gives the table a title based on what the user
 * passed as a props. This is used in the {@link MetadataViewer} and {@link MetadataViewerPdf} components and well as
 * for generating data to export into Excel.
 *
 * @param metadata - The TRAC metadata to show in tables.
 * @param allProcessedAttributes - The dataset of processed attribute definitions. This is created by the {@link processAttributes}
 * function, the dataset is stored in the {@link setAttributesStore}.
 * @param tablesToShow - The tables to break the metadata into, these need to be keys
 * in the metadata object. The user can use this to set the titles for the tables too.
 */
export function createTableData(metadata: undefined | trac.metadata.ITag, tablesToShow: MetadataTableToShow[], allProcessedAttributes: Record<string, UiAttributesProps>): { data: { key: string, value: DataValues | (DataValues)[] | Record<string, DataValues> }[], title: string }[] {

    if (!metadata) return []

    const objectType = metadata.header?.objectType

    // If there is an object type defined go and get all the attributes for that object type, otherwise set an
    // empty object which will mean that no additional formatting will be done
    const processedAttributes = objectType ? getAttributesByObject(allProcessedAttributes, [convertObjectTypeToString(objectType, false)]) : {}

    // In the attrs TRAC adds in the create and update user details, however it adds in both the ID and the
    // username as separate entries, this is four rows of metadata, what we do is remove the IDs and instead
    // create a concatenated the two (ID: 'guest', name: 'Guest user' becomes 'Guest user (guest)'). Here
    // we are going to extract the values before we modify them.
    let trac_create_user_id: string | undefined = undefined
    let trac_update_user_id: string | undefined = undefined
    let trac_create_user_name: string | undefined = undefined
    let trac_update_user_name: string | undefined = undefined

    // The header tag only has the current versions for the object and the tag. However, the attrs has the 'create' and
    // 'update' times of the object. If someone is looking at not the original object version then it is useful to
    // show them the 'create' time as an attribute. Here we are going to extract the value and inject a formatted version.
    let trac_create_time: string | undefined = undefined

    return tablesToShow.map(tableData => {

            let metadataSection = metadata[tableData.key]

            const arrayOfValues = tableData.key && metadataSection ? Object.entries(metadataSection).filter(([key, value]) => {

                // Get the values for the ID and user variables before we filter the ID values out
                if (key === "trac_create_user_id") {
                    let extractedValue = extractValueFromTracValueObject(value)
                    trac_create_user_id = typeof extractedValue.value === "string" ? extractedValue.value : undefined
                } else if (key === "trac_update_user_id") {
                    let extractedValue = extractValueFromTracValueObject(value)
                    trac_update_user_id = typeof extractedValue.value === "string" ? extractedValue.value : undefined
                } else if (key === "trac_create_user_name") {
                    let extractedValue = extractValueFromTracValueObject(value)
                    trac_create_user_name = typeof extractedValue.value === "string" ? extractedValue.value : undefined
                } else if (key === "trac_update_user_name") {
                    let extractedValue = extractValueFromTracValueObject(value)
                    trac_update_user_name = typeof extractedValue.value === "string" ? extractedValue.value : undefined
                } else if (key === "trac_create_time") {
                    let extractedValue = extractValueFromTracValueObject(value)
                    trac_create_time = typeof extractedValue.value === "string" ? extractedValue.value : undefined
                }

                // In TRAC the create and update times are stored in the header but also copied to the attrs to
                // make them searchable, so here we remove the duplicated update time, we keep the created time
                // as that is still useful if the user is not looking at the original object version.
                return !(tableData.key === "attrs" && ["trac_update_time", "trac_create_user_id", "trac_update_user_id"].includes(key))

            }).map(([key, value]) => {

                // This intercepts the key value pairs in the TRAC API metadata object for the
                // header and converts the values to be more humanreadable
                if (tableData.key === "header") return enrichMetadataHeader(key, value)

                // This intercepts the key value pairs in the TRAC API metadata object for the attrs and converts the values to be more humanreadable
                else if (tableData.key === "attrs") {

                    // Inject edited values for the usernames which include the IDs
                    if (key === "trac_create_user_name" && trac_create_user_id && trac_create_user_name && value.stringValue) {
                        return {key: "Created by", value: `${trac_create_user_name} (${trac_create_user_id})`}
                    } else if (key === "trac_update_user_name" && trac_update_user_id && trac_update_user_name && value.stringValue) {
                        return {key: "Updated by", value: `${trac_create_user_name} (${trac_create_user_id})`}
                    } else if (key === "trac_create_time" && trac_create_time && value.datetimeValue) {
                        return {
                            key: "Object created",
                            value: convertIsoDateStringToFormatCode(value.datetimeValue.isoDatetime, "DATETIME")
                        }
                    } else {
                        return enrichMetadataAttr(key, value, processedAttributes)
                    }
                }
                // This is for all other properties of the metadata (not the header and the attrs)
                else return {key: key, value: extractValueFromTracValueObject(value).value}

            }).filter(isDefined) : []

            return ({
                data: sortArrayBy(arrayOfValues, "key"),
                title: tableData.title || tableData.key
            })
        }
    )
}

/**
 * A function that takes the processed attribute/parameter definitions and returns only those that are for string attributes.
 * This excludes array of string attribute/parameter.
 *
 * @param processedAttributes - The attribute/parameter object from the processAttributes function.
 */
export const getStringAttributes = (processedAttributes: Record<string, UiAttributesProps>): string[] => {

    return Object.entries(processedAttributes).filter(([_, value]) => {

        return value.basicType === trac.STRING && !value.useOptions
    }).map(([key]) => key)
}

/**
 * A function that takes the processed attribute/parameter definitions and returns only those that are for boolean attributes.
 * This excludes array of boolean attribute/parameter.
 *
 * @param processedAttributes - The attribute/parameter object from the processAttributes function.
 */
export const getBooleanAttributes = (processedAttributes: Record<string, UiAttributesProps>): string[] => {

    return Object.entries(processedAttributes).filter(([, value]) => {
        return value.basicType === trac.BOOLEAN
    }).map(([key]) => key)
}

/**
 * A function that takes the processed attribute/parameter definitions and returns only those that are for integer attributes.
 * This excludes array of integer attribute/parameter.
 *
 * @param processedAttributes - The attribute/parameter object from the processAttributes function.
 */
export const getIntegerAttributes = (processedAttributes: Record<string, UiAttributesProps>): string[] => {

    return Object.entries(processedAttributes).filter(([, value]) => {

        return value.basicType === trac.INTEGER
    }).map(([key]) => key)
}

/**
 * A function that takes the processed attribute/parameter definitions and returns only those that are for float or decimal attributes.
 * This excludes array of float or decimal attribute/parameter.
 *
 * @param processedAttributes - The attribute/parameter object from the processAttributes function.
 */
export const getFloatAndDecimalAttributes = (processedAttributes: Record<string, UiAttributesProps>): string[] => {

    return Object.entries(processedAttributes).filter(([, value]) => {
        return value.basicType === trac.FLOAT || value.basicType === trac.DECIMAL
    }).map(([key]) => key)
}

/**
 * A function that takes the processed attribute/parameter definitions and returns only those that are for date attributes.
 * This excludes array of dates and datetime attribute/parameter.
 *
 * @param processedAttributes - The attribute/parameter object from the processAttributes function.
 */
export const getDateAttributes = (processedAttributes: Record<string, UiAttributesProps>): string[] => {

    return Object.entries(processedAttributes).filter(([, value]) => {
        return value.basicType === trac.DATE
    }).map(([key]) => key)
}

/**
 * A function that takes the processed attribute/parameter definitions and returns only those that are for datetime attributes.
 * This excludes array of datetimes and date attribute/parameter.
 *
 * @param processedAttributes - The attribute/parameter object from the processAttributes function.
 */
export const getDatetimeAttributes = (processedAttributes: Record<string, UiAttributesProps>): string[] => {

    return Object.entries(processedAttributes).filter(([, value]) => {
        return value.basicType === trac.DATETIME
    }).map(([key]) => key)
}

/**
 * A function that takes the processed attribute/parameter definitions and their values and converts these into am array of tag
 * updates that can be passed to the TRAC API to apply to an object.
 *
 * @remarks This function does not fill in the operation for the tag i.e. 'CREATE_OR_APPEND_ATTR', this means that the default
 * operation will be applied by TRAC.
 *
 * @param processedAttributes - The attribute/parameter object from the processAttributes function.
 * @param values - A set of Select component payloads, keyed by the attribute name.
 */
export const createTagsFromAttributes = (processedAttributes: Record<string, UiAttributesProps>, values: Record<string, SelectPayload<Option, boolean>["value"]>): trac.metadata.ITagUpdate[] => {

    return Object.values(processedAttributes).filter(processedAttributes => values[processedAttributes.id] !== null).map(processedAttribute => {

        return createTag(processedAttribute.id, processedAttribute.basicType, values[processedAttribute.id])
    })
}
/**
 * A function that takes an attribute name, type and value and creates the equivalent TRAC Tag object. These are used in
 * API calls to add tags or attributes to TRAC objects.
 *
 * @param name - The name of the attribute.
 * @param basicType - The TRAC basicType for the attribute
 * @param value - The value to use.
 */
export const createTag = (name: string, basicType: trac.metadata.BasicType, value: null | boolean | string | number | SingleValue<Option> | MultiValue<Option>): trac.metadata.ITagUpdate => {

    let attr: trac.metadata.ITagUpdate = {
        attrName: name,
        value: {}
    }

    // If the attribute is multivalued then we have to create an array type attribute.
    if (isMultiOption(value)) {

        // For each value remove null values, reduce the values to a unique list and get the TRAC object for the
        // value type
        const items = [...new Set(value.filter(item => item.value !== null).map(item => item.value))].map(value => {

            return setTracValue(basicType, value)
        })

        // Wrap the array of values into an array type attribute
        attr.value = {
            type: {basicType: trac.BasicType.ARRAY, arrayType: {basicType: basicType}},
            arrayValue: {
                items: items
            }
        }

    } else {

        // Handle single options and values
        attr.value = {
            ...setTracValue(basicType, isOption(value) ? value.value : value),
            type: {basicType: basicType}
        }
    }

    return attr
}

/**
 * A function that takes the values for any of the Select components and compares them to another to see if the values
 * are identical or not. This is useful when re-running jobs for example, and you need to check if the user has set the
 * same parameters or options as the job to re-run.
 *
 * @param value1 - The first value to compare.
 * @param value2 - The second value to compare.
 */
export const areSelectValuesEqual = (value1: SelectPayload<Option, boolean>["value"], value2: SelectPayload<Option, boolean>["value"]): boolean => {

    if (isPrimitive(value1) && isPrimitive(value2) && value1 === value2) {
        return true
    } else if (isOption(value1) && isOption(value2) && value1.value === value2.value) {
        return true
    } else {
        return Array.isArray(value1) && Array.isArray(value2) && arraysOfPrimitiveValuesEqual(value1.map(item => item.value), value2.map(item => item.value))
    }
}

/**
 * A function that takes an object's metadata tag and extracts the values into a format that can be passed to a Select component,
 * for example if the attribute is for an array of string values then these are converted to a set of options for the
 * SelectOption component. This is used when editing an object's tags in the {@link UpdateTagsScene} for example.
 *
 * @param primitiveValues - An object of primitive values (numbers, strings, booleans) and their array and object equivalents. This is
 * keyed by the name of the attribute.
 * @param processedAttributesForObject - The attributes dataset converted by the processAttributes function. Used to validate the
 * primitive values match the expected type.
 */
export const convertPrimitiveValuesToSelectValues = (primitiveValues: Record<string, ExtractedTracValue>, processedAttributesForObject: Record<string, UiAttributesProps>): Record<string, DeepWritable<SelectPayload<Option, boolean>["value"]>> => {

    // The extracted values that we are going to return
    let selectValues: Record<string, DeepWritable<SelectPayload<Option, boolean>["value"]>> = {}

    Object.keys(primitiveValues).forEach(attributeKey => {

        // Get the definition of the parameter from the downloaded data
        const attributeDefinition = Object.values(processedAttributesForObject).find(attributeDefinition => attributeDefinition.id === attributeKey)

        // Is there a definition for the attributes trying to be set
        if (attributeDefinition !== undefined) {

            const newValue = primitiveValues[attributeKey]

            if (newValue === null) {
                selectValues[attributeKey] = newValue
            } else if (typeof newValue === "string" && attributeDefinition.basicType === trac.DATE && isValidIsoDateString(newValue)) {
                selectValues[attributeKey] = newValue
            } else if (typeof newValue === "string" && attributeDefinition.basicType === trac.DATETIME && isValidIsoDatetimeString(newValue)) {
                selectValues[attributeKey] = newValue
            } else if (typeof newValue === "string" && isTracString(attributeDefinition.basicType)) {
                selectValues[attributeKey] = newValue
            } else if (typeof newValue === "boolean" && isTracBoolean(attributeDefinition.basicType)) {
                selectValues[attributeKey] = newValue
            } else if (typeof newValue === "number" && isTracNumber(attributeDefinition.basicType)) {

                if (attributeDefinition.basicType === trac.INTEGER) {
                    if (roundNumberToNDecimalPlaces(newValue, 0) === newValue) selectValues[attributeKey] = newValue
                } else {
                    selectValues[attributeKey] = newValue
                }

            } else if (Array.isArray(newValue) && (attributeDefinition.specialType === "OPTION" || attributeDefinition.isMulti) && attributeDefinition.options != undefined) {

                if (isGroupOption(attributeDefinition.options)) {

                    let newValueFromAcrossGroups: GenericOption[] = []

                    attributeDefinition.options.forEach(optionGroup => {
                        newValueFromAcrossGroups = [...newValueFromAcrossGroups, ...optionGroup.options.filter(option => option.value != null && newValue.includes(option.value))]
                    })

                    selectValues[attributeKey] = newValueFromAcrossGroups

                } else {

                    selectValues[attributeKey] = attributeDefinition.options.filter(option => option.value != null && newValue.includes(option.value))
                }
            }
        }
    })

    return selectValues
}