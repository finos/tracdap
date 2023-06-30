import {arraysOfPrimitiveValuesEqual} from "../../utils/utils_arrays";
import {
    convertPrimitiveValuesToSelectValues,
    extractDefaultValues,
    getAttributesByObject,
    getBooleanAttributes,
    getDateAttributes,
    getDatetimeAttributes,
    getFloatAndDecimalAttributes,
    getIntegerAttributes,
    getStringAttributes
} from "../../utils/utils_attributes_and_parameters";
import {convertTagAttributesToSelectValues} from "../../utils/utils_trac_metadata";
import {createSlice, current, PayloadAction} from '@reduxjs/toolkit';
import {
    DeepWritable,
    ExtractedTracValue,
    Option,
    SelectDateCheckValidityArgs,
    SelectPayload,
    SelectOptionCheckValidityArgs,
    SelectToggleCheckValidityArgs,
    SelectValueCheckValidityArgs,
    StoreStatus
} from "../../../types/types_general";
import {defaultCheckValidity as defaultCheckValidityDate} from "../../components/SelectDate";
import {defaultCheckValidity as defaultCheckValidityOption} from "../../components/SelectOption";
import {defaultCheckValidity as defaultCheckValidityToggle} from "../../components/SelectToggle";
import {defaultCheckValidity as defaultCheckValidityValue} from "../../components/SelectValue";
import {isKeyOf, isMultiOption, isOption, isTracDateOrDatetime, isTracNumber} from "../../utils/utils_trac_type_chckers";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {UiAttributesProps} from "../../../types/types_attributes_and_parameters";

/**
 * An interface for the 'uses' entry point in the store state.
 */
export interface StoreUser {

    attributes: {
        // The status of the process to convert the attribute list dataset owned by the setupApplicationStore into
        // a set of options. This is needed because if the application state is reset (due to a change in tenant for
        // example) then we need to have a hook to recalculate them. The value of this status is used as that hook.
        status: StoreStatus,
        processedAttributes: Record<string, UiAttributesProps>
        objectTypes: string[]
        values: Record<string, SelectPayload<Option, boolean>["value"]>
        lastAttributeChanged: null | string
        validation: { isValid: Record<string, boolean>, validationChecked: boolean }
    }
}

/**
 * An interface for the setAttributesStore Redux store.
 */
export interface SetAttributesStoreState {
    // An object that has all the attributes set by the user processed into a definition that can be used by the
    // application. The dataset itself is all strings for example, ths parses all the values, options etc. so that the
    // work with the actual components. The business segment dataset is also used to enrich the data.
    // This is the master dataset across all object types.
    allProcessedAttributes: Record<string, UiAttributesProps>
    // The individual use cases of this store, the processedAttributes property in each
    // is a sub set of the allProcessedAttributes master list for the particular use
    uses: {
        uploadADataset: StoreUser
        uploadASchema: StoreUser
        uploadAModel: StoreUser
        uploadAFlow: StoreUser
        attachDocument: StoreUser
        runAFlow: StoreUser
        runAModel: StoreUser
        updateTags: StoreUser
    }
}

// This is the initial state of the store.
//  NOTE If you need to extend the application to use attributes in a new page or component then simply copy the initial
// state to a new property and load that property in the new scene. The existing reducers all the key (called storeKey)
// to set values. You will need to update the type interface for the store above too.
const initialState: SetAttributesStoreState = {
    allProcessedAttributes: {},
    uses: {
        uploadADataset: {
            attributes: {
                status: "idle",
                objectTypes: ["DATA"],
                processedAttributes: {},
                values: {},
                lastAttributeChanged: null,
                validation: {isValid: {}, validationChecked: false}
            }
        },
        uploadASchema: {
            attributes: {
                status: "idle",
                objectTypes: ["SCHEMA"],
                processedAttributes: {},
                values: {},
                lastAttributeChanged: null,
                validation: {isValid: {}, validationChecked: false}
            }
        },
        uploadAModel: {
            attributes: {
                status: "idle",
                objectTypes: ["MODEL"],
                processedAttributes: {},
                values: {},
                lastAttributeChanged: null,
                validation: {isValid: {}, validationChecked: false}
            }
        },
        uploadAFlow: {
            attributes: {
                status: "idle",
                objectTypes: ["FLOW"],
                processedAttributes: {},
                values: {},
                lastAttributeChanged: null,
                validation: {isValid: {}, validationChecked: false}
            }
        },
        attachDocument: {
            attributes: {
                status: "idle",
                objectTypes: ["FILE"],
                processedAttributes: {},
                values: {},
                lastAttributeChanged: null,
                validation: {isValid: {}, validationChecked: false}
            }
        },
        runAFlow: {
            attributes: {
                status: "idle",
                objectTypes: ["JOB"],
                processedAttributes: {},
                values: {},
                lastAttributeChanged: null,
                validation: {isValid: {}, validationChecked: false}
            }
        },
        runAModel: {
            attributes: {
                status: "idle",
                objectTypes: ["JOB"],
                processedAttributes: {},
                values: {},
                lastAttributeChanged: null,
                validation: {isValid: {}, validationChecked: false}
            }
        },
        updateTags: {
            attributes: {
                status: "idle",
                objectTypes: [],
                processedAttributes: {},
                values: {},
                lastAttributeChanged: null,
                validation: {isValid: {}, validationChecked: false}
            }
        }
    }
}

export const setAttributesStore = createSlice({
    name: 'setAttributesStore',
    initialState: initialState,
    reducers: {

        /**
         * A reducer that sets the master list of attributes.
         */
        setAllProcessedAttributes: (state, action: PayloadAction<Record<string, UiAttributesProps>>) => {

            state.allProcessedAttributes = action.payload
        },
        /**
         * A reducer that sets the object types that the attributes are for, this is necessary for scenes like the {@link UpdateTagsScene} where the
         * object type depends on what the user has selected. If the object type changes then the attribute values are reset.
         */
        setObjectTypes: (state, action: PayloadAction<{ storeKey: keyof SetAttributesStoreState["uses"], objectTypes: string[] }>) => {

            const {objectTypes, storeKey} = action.payload

            // We need to check whether to update the list of attributes to a new object type and reset the existing attribute values
            const objectTypesHaveChanged = !arraysOfPrimitiveValuesEqual(objectTypes, state.uses[storeKey].attributes.objectTypes)

            state.uses[storeKey].attributes.objectTypes = objectTypes

            if (objectTypesHaveChanged) {
                setAttributesStore.caseReducers.processAttributesDataForSingleUse(state, {payload: {storeKey}, type: "uploadTagsStore/setObjectTypes"})
            }
        },
        /**
         * A reducer that sets the values of the attributes without the user having to manually do something in the interface. This can be used for
         * example when setting hidden attribute values, these are the pieces of information that we want to add but not show the user. This
         * reducer needs to make sure that it only changes the attributes passed in the arguments, other attributes need to be retained.
         */
        setAttributesAutomatically: (state, action: PayloadAction<{ storeKey: keyof SetAttributesStoreState["uses"], values: Record<string, ExtractedTracValue> }>) => {

            const {storeKey, values} = action.payload

            // Convert the values provided to their Select component equivalent. We use processedAttributes to validate their
            // type is correct and that the options exist
            state.uses[storeKey].attributes.values = {...state.uses[storeKey].attributes.values, ...convertPrimitiveValuesToSelectValues(values, state.uses[storeKey].attributes.processedAttributes)}

            // Update the validation status of the automatically set attributes for a single user in the store
            setAttributesStore.caseReducers.validateAttributesForSingleUse(state, {payload: {storeKey}, type: "setAttributesAutomatically"})
        },
        /**
         * A reducer that takes the user interface attributes dataset and filters it to extract only those that relate to a particular
         * sub set of object types used by each use case in the store (e.g. extracting just those applicable to a job for the
         * {@link RunAFlowScene}) and then converts it to a format used by the ParameterMenu component, so they can be edited in the UI.
         *
         * @remarks Note that the {@link OnLoad} component initiates a request to TRAC to get datasets for the UI that are then
         * stored in the {@link applicationSetupStore} store. One of these datasets is the list of attributes available. The
         * {@link applicationSetupStore} also processes these attributes into a format that can be used by the UI and then uses
         * this function to this store so that all the use cases are populated and default values set without any component using
         * this store needing to be mounted.
         */
        processAttributesDataForAllUses: (state) => {

            // Get the master list of attributes
            const {allProcessedAttributes} = state

            Object.keys(state.uses).forEach(userKey => {

                if (isKeyOf(state.uses, userKey)) {

                    const {objectTypes} = state.uses[userKey].attributes

                    state.uses[userKey].attributes.status = "succeeded"

                    // Get only those attributes that relate to the type needed for the use
                    state.uses[userKey].attributes.processedAttributes = getAttributesByObject(allProcessedAttributes, objectTypes)

                    // Note that if the user changes the attributes definitions and reruns this function as a side effect then
                    // this resets the default value - if they edited an attribute before editing the attributes definitions then they lose that change
                    state.uses[userKey].attributes.values = extractDefaultValues(state.uses[userKey].attributes.processedAttributes)
                }
            })
        },
        /**
         * A reducer that takes the user interface attributes dataset and filters it to extract only those that relate to a particular
         * sub set of object types for a single use case in the store (e.g. extracting just those applicable to a job for the
         * {@link RunAFlowScene}) and then converts it to a format used by the ParameterMenu component, so they can be edited in the UI.
         * This is needed in addition to processAttributesDataForAllUses because in some use cases the object type is dynamic depending
         * on what the user selected.
         *
         * @remarks Note that the {@link OnLoad} component initiates a request to TRAC to get datasets for the UI that are then
         * stored in the {@link applicationSetupStore} store. One of these datasets is the list of attributes available. The
         * {@link applicationSetupStore} also processes these attributes into a format that can be used by the UI and then uses
         * this function to this store so that all the use cases are populated and default values set without any component using
         * this store needing to be mounted.
         */
        processAttributesDataForSingleUse: (state, action: PayloadAction<{ storeKey: keyof SetAttributesStoreState["uses"] }>) => {

            const {storeKey} = action.payload

            // Get the master list of attributes
            const {allProcessedAttributes} = state

            const {objectTypes} = state.uses[storeKey].attributes

            state.uses[storeKey].attributes.status = "succeeded"

            // Get only those attributes that relate to the type needed for the use
            state.uses[storeKey].attributes.processedAttributes = getAttributesByObject(allProcessedAttributes, objectTypes)

            // Note that if the user changes the attributes definitions and reruns this function as a side effect then
            // this resets the default value - if they edited an attribute before editing the attributes definitions then they lose that change
            state.uses[storeKey].attributes.values = extractDefaultValues(state.uses[storeKey].attributes.processedAttributes)
        },
        /**
         * A reducer that takes a tag for an object and converts all the metadata into select component payloads and uses these to
         * set the current values of the attributes in the UI. This is done when editing an object's metadata.
         */
        setAttributesFromTag: (state, action: PayloadAction<{ storeKey: keyof SetAttributesStoreState["uses"], tag: null | trac.metadata.ITag }>) => {

            const {storeKey, tag} = action.payload
            const {processedAttributes} = state.uses[storeKey].attributes

            if (tag) {

                // Convert the values in the attributes into values that can be passed to the
                // select components
                state.uses[storeKey].attributes.values = convertTagAttributesToSelectValues(tag, processedAttributes)
                state.uses[storeKey].attributes.lastAttributeChanged = null
                state.uses[storeKey].attributes.validation.validationChecked = false
                state.uses[storeKey].attributes.validation.isValid = {}

                // Update the validation status of the tag's set attributes
                setAttributesStore.caseReducers.validateAttributesForSingleUse(state, {payload: {storeKey}, type: "setAttributesFromTag"})

            } else {

                state.uses[storeKey].attributes.values = {}
                state.uses[storeKey].attributes.lastAttributeChanged = null
                state.uses[storeKey].attributes.validation.validationChecked = false
                state.uses[storeKey].attributes.validation.isValid = {}
            }
        },
        /**
         * A reducer that takes the user set value for an attribute and stores the value and the validation in the store.
         * Note that we have to make the payload writeable (have no readonly properties) as part of the SelectOptionPayload
         * is read only.
         */
        setAttribute: (state, action: PayloadAction<DeepWritable<SelectPayload<Option, boolean>>>) => {

            const {id, isValid, storeKey, value} = action.payload

            if (isKeyOf(state.uses, storeKey) && typeof id === "string") {

                state.uses[storeKey].attributes.values[id] = value
                state.uses[storeKey].attributes.validation.isValid[id] = isValid
                state.uses[storeKey].attributes.lastAttributeChanged = id
                state.uses[storeKey].attributes.validation.validationChecked = false
            }
        },
        /**
         * A reducer that reviews the values of all attributes for a particular use case and applies the default validity
         * checking functions to them to set their validation status. This is required where we are setting the values of
         * attributes automatically behind the scenes as the Select component validity checking only runs when the
         * components load or when the value is changed in the UI.
         *
         * Note that this does not validate the values themselves, so does not check that the value is an integer or a valid
         * ISO date. That checking of the values is done either by the Select components themselves or the setAttributesAutomatically,
         * setAttributesFromTag or resetAttributesToDefaults reducers.
         */
        validateAttributesForSingleUse: (state, action: PayloadAction<{ storeKey: keyof SetAttributesStoreState["uses"] }>) => {

            const {storeKey} = action.payload

            // The set of attributes that the configured for the object type in the use case
            const {processedAttributes} = state.uses[storeKey].attributes

            const stringAttributes = getStringAttributes(processedAttributes)

            const booleanAttributes = getBooleanAttributes(processedAttributes)
            const integerAttributes = getIntegerAttributes(processedAttributes)
            const floatAndDecimalAttributes = getFloatAndDecimalAttributes(processedAttributes)
            const dateAttributes = getDateAttributes(processedAttributes)
            const datetimeAttributes = getDatetimeAttributes(processedAttributes)

            Object.entries(state.uses[storeKey].attributes.values).forEach(([key, value]) => {

                if (processedAttributes.hasOwnProperty(storeKey)) {

                    if (stringAttributes.includes(key)) {

                        const {maximumValue, minimumValue, mustValidate} = processedAttributes[key]

                        const payload: SelectValueCheckValidityArgs = {
                            maximumValue: typeof maximumValue === "number" ? maximumValue : undefined,
                            minimumValue: typeof minimumValue === "number" ? minimumValue : undefined,
                            mustValidate: mustValidate,
                            basicType: trac.STRING,
                            value: value === null || typeof value === "string" ? value : null
                        }

                        state.uses[storeKey].attributes.validation.isValid[key] = defaultCheckValidityValue(payload).isValid

                    } else if (booleanAttributes.includes(key)) {

                        const payload: SelectToggleCheckValidityArgs = {
                            mustValidate: processedAttributes[key].mustValidate,
                            basicType: trac.BOOLEAN,
                            value: value === null || typeof value === "boolean" ? value : null
                        }

                        state.uses[storeKey].attributes.validation.isValid[key] = defaultCheckValidityToggle(payload).isValid

                    } else if (integerAttributes.includes(key) || floatAndDecimalAttributes.includes(key)) {

                        const {maximumValue, minimumValue, mustValidate, basicType} = processedAttributes[key]

                        if (isTracNumber(basicType)) {

                            const payload: SelectValueCheckValidityArgs = {
                                maximumValue: typeof maximumValue === "number" ? maximumValue : undefined,
                                minimumValue: typeof minimumValue === "number" ? minimumValue : undefined,
                                mustValidate,
                                basicType,
                                value: value === null || typeof value === "string" ? value : null
                            }

                            state.uses[storeKey].attributes.validation.isValid[key] = defaultCheckValidityValue(payload).isValid
                        }

                    } else if (dateAttributes.includes(key) || datetimeAttributes.includes(key)) {

                        const {maximumValue, minimumValue, mustValidate, basicType} = processedAttributes[key]

                        if (isTracDateOrDatetime(basicType)) {

                            const payload: SelectDateCheckValidityArgs = {
                                maximumValue: typeof maximumValue === "string" ? maximumValue : undefined,
                                minimumValue: typeof minimumValue === "string" ? minimumValue : undefined,
                                mustValidate,
                                basicType,
                                value: value === null || typeof value === "string" ? value : null
                            }

                            state.uses[storeKey].attributes.validation.isValid[key] = defaultCheckValidityDate(payload).isValid
                        }

                    } else if ((processedAttributes[key].specialType === "OPTION" || processedAttributes[key].isMulti) && processedAttributes[key].options != undefined) {

                        const {isMulti, mustValidate, basicType} = processedAttributes[key]

                        const payload: SelectOptionCheckValidityArgs = {
                            isMulti,
                            mustValidate,
                            basicType,
                            value: value === null || isOption(value) || isMultiOption(value) ? value : null
                        }

                        state.uses[storeKey].attributes.validation.isValid[key] = defaultCheckValidityOption(payload).isValid
                    }
                }
            })
        },
        /**
         * A reducer that sets whether to show the validation messages for the attributes, usually when the user sets to save something.
         */
        setValidationChecked: (state, action: PayloadAction<{ storeKey: keyof SetAttributesStoreState["uses"], value: boolean }>) => {

            const {storeKey, value} = action.payload
            state.uses[storeKey].attributes.validation.validationChecked = value
        },
        /**
         * A reducer that resets attributes back to their default values.
         */
        resetAttributesToDefaults: (state, action: PayloadAction<{ storeKey: keyof SetAttributesStoreState["uses"] }>) => {

            const {storeKey} = action.payload

            state.uses[storeKey].attributes.values = extractDefaultValues(state.uses[storeKey].attributes.processedAttributes)
            state.uses[storeKey].attributes.validation.validationChecked = false
            state.uses[storeKey].attributes.lastAttributeChanged = null
            state.uses[storeKey].attributes.validation.isValid = {}

            // Update the validation status of the original attributes
            setAttributesStore.caseReducers.validateAttributesForSingleUse(state, {payload: {storeKey}, type: "resetAttributesToDefaults"})
        }
    }
})

// Action creators are generated for each case reducer function
export const {
    processAttributesDataForAllUses,
    resetAttributesToDefaults,
    setAllProcessedAttributes,
    setAttribute,
    setAttributesAutomatically,
    setAttributesFromTag,
    setObjectTypes,
    setValidationChecked
} = setAttributesStore.actions

export default setAttributesStore.reducer