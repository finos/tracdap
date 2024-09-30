/**
 * A component that shows a select box allowing the user to select one or more items from a list. It is based on the
 * react-select package that gives far more functionality than the native select component in forms. For example, it
 * can do multi select, search and allows users to add their own options.
 *
 * @module SelectOption
 * @category Component
 */

import {aOrAn, commasAndAnds, convertObjectKeyToTagSelector, doesOptionExist, hasOptions, isOptionSame, showToast, sOrNot} from "../utils/utils_general";
import {
    CheckValidityReturn,
    CreateDetails,
    GenericGroup,
    GenericOption,
    IsMulti,
    OnCreateNewOptionPayload,
    Option,
    SearchOption,
    SelectOptionCheckValidityArgs,
    SelectOptionPayload,
    SelectOptionProps
} from "../../types/types_general";
import {convertIsoDateStringToFormatCode} from "../utils/utils_formats";
import {convertObjectTypeToString, convertSearchResultsIntoOptions} from "../utils/utils_trac_metadata";
// This Creatable version of the select plugin allows us to create options using an async call
// this means we can make API calls as part of the option creation
import Creatable from "react-select/creatable";
// Import a type/interface for the filterOption prop in the react-select component
import type {FilterOptionOption} from "react-select/dist/declarations/src/filters";
import Form from "react-bootstrap/Form";
import {getObjectsFromUserDefinedObjectIds} from "../utils/utils_trac_api";
import {Icon} from "./Icon";
import type {IndicatorsContainerProps, ValueContainerProps} from "react-select/dist/declarations/src/components/containers";
import {isObjectId, isObjectKey} from "../utils/utils_string";
import {isSingleValue, isTagOption} from "../utils/utils_trac_type_chckers";
import {LabelLayout} from "./LabelLayout";
import type {MenuProps} from "react-select/dist/declarations/src/components/Menu";
import type {OnChangeValue, StylesConfig} from "react-select";
import Select, {components} from "react-select";
import type {OptionProps} from "react-select/dist/declarations/src/components/Option";
import PropTypes from "prop-types";
import React, {memo, useCallback, useEffect, useMemo, useRef, useState} from "react";
// Import a type/interface for the component prop in the react-select component
import type {SelectComponents} from "react-select/dist/declarations/src/components";
import {toast} from "react-toastify";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../types/types_hooks";
import type {SingleValueProps} from "react-select/dist/declarations/src/components/SingleValue";

/**
 * The default function for the checkValidity prop. This is defined outside the component
 *  in order to prevent re-renders.
 * *
 *  * @param payload - The information required to check if the value is valid.
 *  */
export const defaultCheckValidity = (payload: SelectOptionCheckValidityArgs): CheckValidityReturn => {

    const {
        isMulti,
        mustValidate,
        value
    } = payload

    if (mustValidate && !isMulti && value == null) {
        return {isValid: false, message: `Please select an option`}
    } else if (mustValidate && isMulti && Array.isArray(value) && value.length === 0) {
        return {isValid: false, message: `Please select at least one option`}
    } else {
        return {isValid: true, message: ""}
    }
}

// The default function for the filterOption prop. This is defined outside the component
// in order to prevent re-renders.
const defaultFilterOption = (option: FilterOptionOption<Option>, inputValue: string, hideDisabledOptions: boolean): boolean => {

    // Hide due to disabled status
    const hide = Boolean(hideDisabledOptions && option.data.disabled)

    // If the user has entered a valid object ID then search only the object ID field which should be the value,
    // the value is <OBJECT_TYPE>-<OBJECT_ID>-v<OBJECT_VERSION>-v<TAG_VERSION> and is created by the
    // createUniqueObjectFingerprint function
    if (isTagOption(option.data) && option.data.tag.header) {

        const {objectId, objectTimestamp, objectVersion, tagTimestamp} = option.data.tag.header

        if (isObjectId(inputValue) && objectId && !hide) {

            return objectId.toLowerCase().indexOf(inputValue.toLowerCase()) > -1
        }
        // If the user has entered a valid object key then search only the object ID and version field
        else if (isObjectKey(inputValue) && objectId && objectVersion != null && !hide) {

            const tagSelector = convertObjectKeyToTagSelector(inputValue)
            // TODO this has a bug that if the object key says "latest" then the version isn't known
            return !tagSelector ? false : Boolean(objectId === tagSelector.objectId && objectVersion === tagSelector.objectVersion)
        }
            // If the user has entered a valid datetime then search only the object ID field. This datetime
            // is usually in the label and this would not be needed - but this is in case the datetime
        // is removed from the label for whatever reason
        else if (objectTimestamp?.isoDatetime != null && tagTimestamp?.isoDatetime != null && !hide) {

            return option.label.toLowerCase().indexOf(inputValue.toLowerCase()) > -1 || (tagTimestamp.isoDatetime.indexOf(inputValue) > -1 || convertIsoDateStringToFormatCode(tagTimestamp.isoDatetime, "DATETIME").toLowerCase().indexOf(inputValue.toLowerCase()) > -1 || objectTimestamp.isoDatetime.indexOf(inputValue) > -1 || convertIsoDateStringToFormatCode(objectTimestamp.isoDatetime, "DATETIME").toLowerCase().indexOf(inputValue.toLowerCase()) > -1) && !hide

        } else {

            // Default is to return the basic search of the label
            return option.label.toLowerCase().indexOf(inputValue.toLowerCase()) > -1 && !hide
        }
    }
    // Default is to return the basic search of the label
    else {

        return option.label.toLowerCase().indexOf(inputValue.toLowerCase()) > -1 && !hide
    }
}
// The default function for the isOptionDisabled prop. This is defined outside the component
// in order to prevent re-renders.
const defaultIsOptionDisabled = (option: Option): boolean => {

    return Boolean(option.disabled);
}

// The default for the options prop. This is defined outside the component
// in order to prevent re-renders.
const defaultOptions: SelectOptionProps["options"] = []

/**
 * A function that is passed as a prop to the react-select component that sets the
 * message to show if there are no options.
 */
function noOptionsMessage(): string {

    return "No options are available";
}

/**
 * A function that sets the placeholder text in the select, this can be overridden by passing the
 * placeHolderText prop otherwise it will show a message based on the type of select and the
 * number of options.
 *
 * @param placeHolderText - The default placeholder text for the component.
 * @param isCreatable - Whether the user can create additional options.
 * @param hideDropDown - Whether to hide the dropdown, making the component look like a text input.
 * @param options - The list of options available.
 * @returns - The placeholder text.
 */
const setPlaceHolder = (placeHolderText: string | undefined, isCreatable: boolean, hideDropDown: boolean, options: GenericOption[] | GenericGroup[]): string => {

    if (placeHolderText) {

        return placeHolderText

    } else if (isCreatable && hideDropDown) {

        return "Enter text and press tab to add an option"

    } else if (hasOptions(options)) {
        // We have to work out if the select has options, but it could be an array of groups,
        // so we have a little function that can look for available options even if it is an array of groups
        return "Please select"

    } else {

        return "No available options"
    }
};

/**
 * A function that returns a style object to pass to the select component. This defines all the theme based css to use in the
 * component. The rest of the css for the component is in trac_ui.scss so that it can inherit the bootstrap css.
 * @returns - The style object.
 */
function setStyle(): StylesConfig<Option, IsMulti, GenericGroup> {

    // The function returns a custom style object that is passed to the react-select to
    // change its appearance

    return ({

        control: (base, {isFocused, isDisabled}) => {

            // Most of the css for the main control is in the legacy.css file so that the Bootstrap sass variables
            // can be used to make it look like bootstrap automatically.
            // No class is set on the control to say it is focused. so we need to add it here as a specific rule
            const overlay = isFocused ? {borderColor: "var(--bs-info) !important"} : {}
            return {...base, ...overlay, backgroundColor: isDisabled ? "var(--bs-form-control-disabled-bg)" : "none"}
        },
        option: (base, {isSelected, isFocused, isDisabled}) => ({

            ...base,
            // TODO fix these colour to something suitable
            backgroundColor: isSelected ? "var(--info)" : isFocused ? "var(--focused-background)" : isDisabled ? "var(--disabled-background)" : undefined,
            ":active": {
                ...base[":active"],
                backgroundColor: "var(--active-background)",
                color: "var(--primary-background)"
            },
            color: isDisabled ? "var(--disabled-text)" : isSelected ? "var(--primary-background)" : "var(--primary-text)"
        })
    })
}

const SelectOptionInner = (props: SelectOptionProps) => {

    const {
        basicType,
        checkValidity = defaultCheckValidity,
        className = "",
        dispatchedOnChange,
        filterOption = defaultFilterOption,
        hideDisabledOptions = false,
        hideDropdown = false,
        id,
        index,
        isClearable = false,
        isCreatable = false,
        isDispatched = true,
        isDisabled = false,
        isLoading = false,
        isMulti = false,
        isOptionDisabled = defaultIsOptionDisabled,
        maximumSelectionsBeforeMessageOverride = 5,
        mustValidate = false,
        name,
        objectType,
        onChange,
        onCreateNewOption,
        options = defaultOptions,
        placeHolderText,
        showValidationMessage = false,
        size,
        storeKey,
        useMenuPortalTarget = false,
        validateOnMount = true,
        validationChecked = false,
        // A default value is needed as if value is undefined the component will be rendered as an
        // uncontrolled select which can have unintended consequences
        value = null
    } = props;

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {"trac-tenant": tenant} = useAppSelector(state => state["applicationStore"].cookies)
    const {searchAsOf} = useAppSelector(state => state["applicationStore"].tracApi)

    // Does the option meet the required value
    const isValidValue = checkValidity({
        basicType,
        id,
        isMulti,
        mustValidate,
        name,
        value
    })

    /**
     * A wrapper function that runs when the user changes their selection(s), it runs the
     * function to update the value in the store. useCallback is a performance optimisation.
     *
     * @param newValue - The options selected, if it is a single option it
     * will be an object, if it is a multi select then it will be an array of objects.
     */
    const handleOptionChange = useCallback((newValue: OnChangeValue<Option, IsMulti>): void => {

        // Do nothing in the option didn't actually change, always run on load. If multiple options
        // are selected we always update. isSingleValue is a function that asserts the type so that it
        // matches the signature of the isOptionSame function. This is a rendering optimisation 
        // so that the store only updates when the value actually changes.
        if (isSingleValue(newValue) && isSingleValue(value) && isOptionSame(newValue, value)) return

        // Is the selected option valid
        const isValidValue = checkValidity({
            basicType,
            id,
            isMulti,
            mustValidate,
            name,
            value: newValue
        })

        // Return the payload
        const payload: SelectOptionPayload<Option, IsMulti> = {
            basicType,
            id,
            index,
            isValid: isValidValue.isValid,
            name,
            storeKey,
            value: newValue
        }

        // This passes the options selected, the name of the select and its ID back as an object.
        // This is passed back to state via the redux dispatch/reducer pattern.
        if (isDispatched && dispatchedOnChange == undefined) {
            dispatch(onChange(payload))
        } else if (isDispatched && dispatchedOnChange != undefined) {
            dispatch(dispatchedOnChange(payload))
        } else {
            onChange(payload)
        }

    }, [basicType, checkValidity, dispatch, dispatchedOnChange, id, index, isDispatched, isMulti, mustValidate, name, onChange, storeKey, value])

    /**
     * A wrapper function that runs when the user creates their own option, it runs a function to add the
     * option to the store. Options can be created in one of two instances. In the first, the user is just trying to set
     * their own value for something such as values for an attribute. In this case the value they set is open-ended
     * with some constraints enforced by the application. In the other, we allow users to paste an object ID and for that
     * to be added as an option. The objectType prop enforces what type of object can be loaded as an option. This
     * function handles the second case.
     *
     * @param inputValue - The user set value for the option.
     */
    const handleCreateOption = useCallback((inputValue: string): void => {

        // In some components we use double pipe to concatenate values together, so we strip those out of the user entry
        // as we don't want to store them with the actual delimiter
        const inputWithoutPipes = inputValue.replace(new RegExp(/\|+/g, 'g'), "")

        // Convert the user defined string into an option
        const newOptions: Option<string, CreateDetails> = {
            value: inputWithoutPipes,
            label: inputWithoutPipes,
            details: {userAdded: true}
        }

        // Return the payload
        const payload: OnCreateNewOptionPayload<CreateDetails> = {
            basicType,
            id,
            name,
            storeKey,
            inputValue,
            currentOptions: options,
            newOptions
        }

        // This passes the new option, the name of the select and its ID back as an object.
        // This is passed back to the store via the redux dispatch/reducer pattern.
        if (onCreateNewOption) {

            // Assume that all create options are dispatched to a store
            dispatch(onCreateNewOption(payload))

        } else {
            showToast("error", "No onCreateNewOption function set", "handleCreateOption/rejected")
        }

        // Set the selected option to the new option, when isMulti is true all selected options are passed to the onChange function
        const newValuesSelected = isSingleValue(value) ? newOptions : value == null ? [newOptions] : [...value, newOptions]

        // Set the selected value to the new option
        handleOptionChange(newValuesSelected)

    }, [basicType, dispatch, handleOptionChange, id, name, onCreateNewOption, options, storeKey, value])

    /**
     * A function that runs when the user creates their own option, it runs the function to add the
     * option to the store. Options can be created in one of two instances. In the first the user is just trying to set
     * their own value for something such as values for an attribute. In this case the value they set is open-ended
     * with some constraints enforced by the application. In the other we allow users to paste an object ID and for that
     * to be added as an option. The objectType prop enforces what type of object can be loaded as an option. This
     * function handles the second case.
     *
     * @param inputValue - The user set object ID or key that they want to load.
     * @param oneOrMany - Whether the user can load one or multiple items in one go.
     */
    const handleAddTracObject = useCallback(async (inputValue: string): Promise<void> => {

        // This is just for Typescript to be happy objectType is set although this function can only run if it is.
        // If the user has not set a tenant then the API call can not be made.
        if (!objectType || tenant === undefined) return

        // This is a string used in messaging e.g. "model" or "dataset"
        const objectTypeAsString = convertObjectTypeToString(objectType, true, true)

        // Used in messaging to make it good English
        const aOraN = aOrAn(objectTypeAsString)

        // Used in messaging
        const inputIsAnObjectKey = isObjectKey(inputValue)

        // Turn on the loading status
        setLoadingData(true)

        // Go and do the API calls
        const {
            notAnObjectIdOrKey,
            notCorrectObjectType,
            foundTags,
            notFoundTags,
            suggestedTagForNotFound
        } = await getObjectsFromUserDefinedObjectIds({inputValue, objectType, oneOrMany: "one", searchAsOf, tenant})

        // Clear all the messages, so it's not confusing if the last attempt failed, but they did not clear the messages
        toast.dismiss()

        // If we found that the string was not a valid object ID or key we tell the user and do nothing
        if (notAnObjectIdOrKey.length > 0) {

            showToast("warning", "That is not a valid object ID or object key", "handleAddTracObject/invalid-id-or-key")

            // Turn off the loading status
            setLoadingData(false)

            // Do not continue if there is nothing to continue with
            return
        }

        // If we found that the string was an object key but not the same type that we are allowed to search for
        // then tell the user and do nothing
        if (notCorrectObjectType.length > 0) {

            const foundTypes = commasAndAnds(notCorrectObjectType.map(tagSelector => convertObjectTypeToString(tagSelector?.objectType || trac.ObjectType.OBJECT_TYPE_NOT_SET)))

            const text = `The object key is for ${foundTypes}, but only  ${aOraN} ${objectTypeAsString} can be added to this list`

            showToast("warning", text, "handleAddTracObject/invalid-object-types")

            // Turn off the loading status
            setLoadingData(false)

            // Do not continue if there is nothing to continue with
            return
        }

        // Convert the tag that was found to an option for the SelectOption component
        const newOptions: SearchOption[] = convertSearchResultsIntoOptions(foundTags, false, true, true, false)

        // Are there any results that are already in the options
        const alreadyExists = newOptions.find(newOption => doesOptionExist(newOption, options))

        // Tell the user if we found duplicates and do nothing
        if (alreadyExists) {

            const text = `The ${objectTypeAsString} is already in the list with object ID ${alreadyExists.tag.header?.objectId}`

            showToast("success", text, "handleAddTracObject/fulfilled")

            // Turn off the loading status
            setLoadingData(false)

            // Set the selected option to the one we already found in the list
            handleOptionChange(alreadyExists)
            return
        }

        // Do we have any failed requests that we for a specific version - earlier versions might exist
        if (notFoundTags.length > 0) {

            if (suggestedTagForNotFound) {
                showToast("warning", `That is a valid object ${inputIsAnObjectKey ? "key" : "ID"} but there does not appear to be a version ${notFoundTags[0].objectVersion}, it looks like the latest version is ${suggestedTagForNotFound.header?.objectVersion}.`, "create-option-error")
            } else {
                showToast("warning", `That is a valid object ${inputIsAnObjectKey ? "key" : "ID"} but there does not appear to be ${aOraN} ${objectTypeAsString} with that ${inputIsAnObjectKey ? "key" : "ID"}.`, "create-option-error")
            }

            // Turn off the loading status
            setLoadingData(false)

            return
        }

        //Return the payload
        const payload: OnCreateNewOptionPayload<trac.metadata.ITag> = {
            basicType,
            id,
            name,
            storeKey,
            inputValue,
            currentOptions: options,
            newOptions: newOptions
        }

        // This passes the new option, the name of the select and its ID back as an object.
        // This is passed back to the store via the redux dispatch/reducer pattern.
        if (onCreateNewOption && newOptions.length > 0) {

            // Assume that all create options are dispatched to a store
            dispatch(onCreateNewOption(payload))

        } else if (!onCreateNewOption) {
            showToast("error", "No onCreateNewOption function set", "handleAddTracObject/rejected")
        }

        // Turn off the loading status
        setLoadingData(false)

    }, [basicType, dispatch, handleOptionChange, id, name, objectType, onCreateNewOption, options, searchAsOf, storeKey, tenant])

    /**
     * An object passed to the react-select component that contains replacement functions for rendering
     * components that make up the elements of the react-select component. For example, instead of showing the options
     * selected by a user we can change what is shown when the number of selections gets above a certain number.
     * This is useful when you have a lot of selections. Also, the drop-down is hidden when the user is creating
     * options by there are no options available. We also use this to insert icons into the options.
     * See https://github.com/JedWatson/react-select/issues/2790
     */
    const ReplacementComponents = useMemo((): Partial<SelectComponents<Option, boolean, GenericGroup>> => {

        let newComponents: Partial<SelectComponents<Option, boolean, GenericGroup>> = {}

        // Turn off the dropdown menu list and dropdown icon if there are no options
        // Eslint prefers we set a name for a function rather than passing anonymous functions
        // eslint-disable-next-line react/display-name
        newComponents.Menu = (payload: MenuProps<Option, boolean, GenericGroup>) => {

            const {children, ...payloadProps} = payload

            return (
                hideDropdown && payload.getValue().length === 0 ? null : <components.Menu{...payloadProps}>
                    {children}
                </components.Menu>
            )
        }

        // Eslint prefers we set a name for a function rather than passing anonymous functions
        // eslint-disable-next-line react/display-name
        newComponents.IndicatorsContainer = (payload: IndicatorsContainerProps<Option, boolean, GenericGroup>) => {

            const {children, ...payloadProps} = payload

            return (
                hideDropdown && payload.getValue().length === 0 ? null :
                    <components.IndicatorsContainer{...payloadProps}>
                        {children}
                    </components.IndicatorsContainer>
            )
        }

        // Eslint prefers we set a name for a function rather than passing anonymous functions
        // eslint-disable-next-line react/display-name
        newComponents.ValueContainer = (payload: ValueContainerProps<Option, boolean, GenericGroup>) => {

            const {children, ...payloadProps} = payload

            const numberOfSelections = payloadProps.getValue().length

            // We need to take the original value container and modify the selected options (which are in the child prop)
            // and then replace them all with a single message. Note that {children, ...payloadProps} means that children is
            // removed from payloadProps, so it is not passed down but the new child is.
            return (<components.ValueContainer {...payloadProps}>
                    {maximumSelectionsBeforeMessageOverride && numberOfSelections > maximumSelectionsBeforeMessageOverride && !payloadProps.selectProps.menuIsOpen ?
                        <React.Fragment>
                            <span className={"ms-2"}>
                                {`${numberOfSelections} item${sOrNot(numberOfSelections)} selected (click to view)`}
                            </span>
                            {/*// We need to remove the first child as that will be the options which we just replaced with the message, but we still need to*/}
                            {/*show the other child component that are used in the component.*/}
                            {Array.isArray(children) && React.isValidElement(children[1]) ? React.cloneElement(children[1]) : null}
                        </React.Fragment>
                        : children
                    }
                </components.ValueContainer>
            )
        }

        // Eslint prefers we set a name for a function rather than passing anonymous functions
        // eslint-disable-next-line react/display-name
        newComponents.SingleValue = (payload: SingleValueProps<Option, boolean, GenericGroup>) => {

            const {children, ...payloadProps} = payload

            // We need to take the original single value selected option and modify the option (which is the child prop)
            // and then modify it to show an icon. Note that {children, ...payloadProps} means that children is
            // removed from payloadProps, so it is not passed down but the new child is.
            return (<components.SingleValue {...payloadProps}>
                    {payloadProps.data.icon ?
                        <div className={"d-flex justify-content-between"}>
                            {children}
                            <Icon ariaLabel={"In policy"}
                                  className={"mx-1 text-success"}
                                  colour={null}
                                  icon={payloadProps.data.icon}
                                  tooltip={"In policy"}
                            />
                        </div>
                        : children
                    }
                </components.SingleValue>
            )
        }

        // Eslint prefers we set a name for a function rather than passing anonymous functions
        // eslint-disable-next-line react/display-name
        newComponents.Option = (payload: OptionProps<Option, boolean, GenericGroup>) => {

            const {...payloadProps} = payload

            return (<components.Option {...payloadProps}>
                    {payloadProps.data.icon ?
                        <div className={"d-flex justify-content-between"}>
                            <div>{payloadProps.data.label}</div>
                            <Icon ariaLabel={"In policy"}
                                  className={"mx-1"}
                                  icon={payloadProps.data.icon}
                                  tooltip={"In policy"}
                            />
                        </div>
                        : payloadProps.data.label
                    }
                </components.Option>
            )
        }

        return newComponents

    }, [hideDropdown, maximumSelectionsBeforeMessageOverride])

    // Get the style to apply to the select component
    const styleToUse = useMemo(() => setStyle(), [])

    // Get the text to show when no options are selected
    const finalPlaceHolderText = useMemo(() => setPlaceHolder(placeHolderText, isCreatable, hideDropdown, options), [placeHolderText, isCreatable, hideDropdown, options])

    // Whether an API call is being made
    const [loadingData, setLoadingData] = useState<boolean>(false)

    /**
     * Awesomeness. A hook that runs when the component mounts that sends the value and the validity status of the
     * value to the store. This starts with this ref.
     */
    const componentIsMounting = useRef(true)

    /**
     * This runs when this component loads. This is needed because of form validation. Before we allow some
     * form elements to be used we need to validate their value in the store. But if a form item is given a value
     * upon loading and the user clicks 'send' we haven't got any validation in the store about whether
     * to allow this value.
     */
    useEffect(() => {

        // Strictly only ever run this when the component mounts
        if (componentIsMounting.current) {
            if (validateOnMount) handleOptionChange(value)
            componentIsMounting.current = false
        }

        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [validateOnMount])

    /**
     * There are props that if the value of them changes can alter the validation status of the
     * value, here we have a hook that runs IF the component is updating (rather than mounting)
     * and any of the values that are affect the validation change.
     */
    useEffect(() => {

        if (!componentIsMounting) handleOptionChange(value)

        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [mustValidate, isMulti])

    // Class to show the invalid status, shows when the check made indicates an issue and when there are no options for something that is needed
    const finalClassName = (validationChecked && !isValidValue.isValid) || (mustValidate && !hasOptions(options)) ? `${className} custom-react-select is-invalid` : `custom-react-select ${size ? `custom-react-select-${size}` : ""} ${className}`

    return (
        <LabelLayout {...props}>

            <React.Fragment>
                {/*User can not create options and have not provided a bespoke filter function*/}
                {!isCreatable &&

                    <Select className={finalClassName}
                            components={ReplacementComponents}
                            filterOption={(option, inputValue) => filterOption(option, inputValue, hideDisabledOptions)}
                            isClearable={isClearable}
                            isDisabled={isDisabled || loadingData}
                            isLoading={isLoading || loadingData}
                            isMulti={isMulti}
                            isOptionDisabled={isOptionDisabled}
                            isSearchable={true}
                            onChange={handleOptionChange}
                            options={options}
                            noOptionsMessage={noOptionsMessage}
                            placeholder={finalPlaceHolderText}
                            styles={styleToUse}
                            value={value}
                        // This mounts the menu into a special div with the ID 'custom-react-select'. This
                        // is used when otherwise the menu would be clipped because it is mounted in a table for
                        // example. This div is created in App.tsx, there is more detail on this there. It is
                        // the Table component that uses this prop.
                            menuPortalTarget={useMenuPortalTarget ? document.getElementById('custom-react-select') : undefined}
                    />
                }

                {isCreatable &&
                    <Creatable className={finalClassName}
                               components={ReplacementComponents}
                               createOptionPosition={"first"}
                               filterOption={(option, inputValue) => filterOption(option, inputValue, hideDisabledOptions)}
                               formatCreateLabel={(inputValue: string) => objectType ? `Add ${convertObjectTypeToString(objectType, true, true)} "${inputValue}"` : `Create "${inputValue}"`}
                               isClearable={isClearable}
                               isDisabled={isDisabled || loadingData || isLoading}
                               isLoading={isLoading || loadingData}
                               isMulti={isMulti}
                               isOptionDisabled={isOptionDisabled}
                               isSearchable={true}
                        // Only show the 'Create new...' text if the input has a value, if an object type is set the input needs to be an object key or ID
                               isValidNewOption={(inputValue) => (inputValue != null && inputValue !== "" && (objectType == null || isObjectId(inputValue) || isObjectKey(inputValue)))}
                               noOptionsMessage={noOptionsMessage}
                               onChange={handleOptionChange}
                               onCreateOption={objectType ? handleAddTracObject : handleCreateOption}
                               options={options}
                               placeholder={finalPlaceHolderText}
                               styles={styleToUse}
                               value={value}
                    />
                }

                {showValidationMessage &&
                    <Form.Control.Feedback type="invalid" className={"d-block"}>
                        {validationChecked && !isValidValue.isValid ? isValidValue.message : <br/>}
                    </Form.Control.Feedback>
                }
            </React.Fragment>

        </LabelLayout>
    )
};

SelectOptionInner.propTypes = {

    basicType: PropTypes.number.isRequired,
    checkValidity: PropTypes.func,
    className: PropTypes.string,
    dispatchedOnChange: PropTypes.func,
    filterOption: PropTypes.func,
    helperText: PropTypes.string,
    hideDisabledOptions: PropTypes.bool,
    hideDropdown: PropTypes.bool,
    id: PropTypes.oneOfType([PropTypes.number, PropTypes.string, PropTypes.object]),
    index: PropTypes.number,
    isClearable: PropTypes.bool,
    isCreatable: PropTypes.bool,
    isDisabled: PropTypes.bool,
    isDispatched: PropTypes.bool,
    isMulti: PropTypes.bool,
    isOptionDisabled: PropTypes.func,
    labelPosition: PropTypes.oneOf(["top", "left"]),
    labelText: PropTypes.string,
    maximumSelectionsBeforeMessageOverride: PropTypes.number,
    mustValidate: PropTypes.bool,
    name: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
    onChange: PropTypes.func.isRequired,
    onCreateNewOption: PropTypes.func,
    options: PropTypes.oneOfType([

        PropTypes.arrayOf(PropTypes.shape({
            value: PropTypes.oneOfType([PropTypes.array, PropTypes.number, PropTypes.bool, PropTypes.string]),
            label: PropTypes.string.isRequired,
            disabled: PropTypes.bool,
            type: PropTypes.number,
            details: PropTypes.object,
            tag: PropTypes.object,
            tagHeader: PropTypes.object
        })).isRequired,
        PropTypes.arrayOf(
            PropTypes.shape({
                    label: PropTypes.string,
                    options: PropTypes.arrayOf(
                        PropTypes.shape({
                            value: PropTypes.oneOfType([PropTypes.array, PropTypes.number, PropTypes.bool, PropTypes.string]).isRequired,
                            label: PropTypes.string.isRequired,
                            disabled: PropTypes.bool,
                            type: PropTypes.number,
                            details: PropTypes.object,
                            tag: PropTypes.object,
                            tagHeader: PropTypes.object

                        }).isRequired
                    )
                }
            )
        ).isRequired
    ]),
    placeHolderText: PropTypes.string,
    showValidationMessage: PropTypes.bool,
    size: PropTypes.string,
    tooltip: PropTypes.string,
    validationChecked: PropTypes.bool,
    value: PropTypes.oneOfType([
        PropTypes.arrayOf(
            PropTypes.shape({
                value: PropTypes.oneOfType([PropTypes.array, PropTypes.number, PropTypes.bool, PropTypes.string]),
                label: PropTypes.string.isRequired,
                disabled: PropTypes.bool,
                type: PropTypes.number,
                details: PropTypes.object,
                tag: PropTypes.object,
                tagHeader: PropTypes.object
            })
        ),
        PropTypes.shape({
            value: PropTypes.oneOfType([PropTypes.array, PropTypes.number, PropTypes.bool, PropTypes.string]),
            label: PropTypes.string.isRequired,
            disabled: PropTypes.bool,
            type: PropTypes.number,
            details: PropTypes.object,
            tag: PropTypes.object,
            tagHeader: PropTypes.object
        })
    ])
};

export const SelectOption = memo(SelectOptionInner);
