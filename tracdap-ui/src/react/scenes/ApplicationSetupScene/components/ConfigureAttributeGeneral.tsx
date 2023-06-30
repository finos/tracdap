/**
 * A component that shows the 'GeneralChartConfig' section of the {@link ConfigureAttributeModal} component, this allows
 * the user to edit an attribute so that its options and object types can be set.
 * @module
 * @category Component
 */

import {addUserSetCategory} from "../store/applicationSetupStore";
import Col from "react-bootstrap/Col";
import {convertAnythingToStringArray, isAttributeNameValid,} from "../../../utils/utils_general";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {includes, isDefined} from "../../../utils/utils_trac_type_chckers";
import {CheckValidityReturn, Option, SelectOptionPayload, SelectValueCheckValidityArgs, SelectValuePayload,} from "../../../../types/types_general";
import PropTypes from "prop-types";
import React, {useCallback, useMemo} from "react";
import Row from "react-bootstrap/Row";
import {SelectOption} from "../../../components/SelectOption";
import {SelectValue} from "../../../components/SelectValue";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {Types} from "../../../../config/config_trac_classifications";
import {UiAttributesListRow} from "../../../../types/types_attributes_and_parameters";
import {useAppSelector} from "../../../../types/types_hooks";
import {convertArrayToOptions} from "../../../utils/utils_arrays";

// A set of options for the width multiplier, this makes attributes wider in the UI
const widthMultiplierOptions: Option<number>[] = [{value: 1, label: "Small"}, {value: 2, label: "Medium"}, {value: 3, label: "Large"}]

/**
 * An interface for the props of the ConfigureAttributeGeneral component.
 */
interface Props {

    /**
     * The attribute being edited.
     */
    attribute: UiAttributesListRow
    /**
     * Labels for each form element based on the label property in the dataset's schema.
     */
    labels: Record<string, string>
    /**
     * A hook to set the state in the parent component.
     */
    setAttribute: React.Dispatch<React.SetStateAction<null | UiAttributesListRow>>
    /**
     * A hook to set the state in the parent component.
     */
    setValidation: React.Dispatch<React.SetStateAction<Partial<Record<keyof UiAttributesListRow, boolean>>>>
    /**
     * Whether to form has been validated and show any in validation messages.
     */
    validationChecked: boolean
}

export const ConfigureAttributeGeneral = (props: Props) => {

    const {attribute, validationChecked, labels, setAttribute, setValidation} = props

    // Get what we need from the store
    // User defined categories in addition to those already define in the dataset
    const {data, index, userAddedCategoryOptions} = useAppSelector(state => state["applicationSetupStore"].editor.items.ui_attributes_list)

    /**
     * An array that contains options for all the categories already set across the attributes plus any additional
     * ones that the user has added using the form. Categories allow attributes to be shown in sections in the UI.
     * useMemo means that the values are never recalculated.
     */
    const categoryOptions = useMemo((): Option<string>[] => {

        // Make the list unique
        // The check on isDefined removes null values
        return convertArrayToOptions([...new Set([...data.map(row => row.CATEGORY).filter(isDefined), ...userAddedCategoryOptions.map(option => option.value)])].sort(), false)

    }, [data, userAddedCategoryOptions])

    /**
     * A set of options for all the primitive TRAC data types, an ALL option is added at the start. useMemo
     * means that the values are never recalculated.
     */
    const tracObjectTypesWithAll = useMemo((): Option<string, trac.ObjectType>[] => [{
        value: "ALL",
        label: "All",
        type: trac.ObjectType.OBJECT_TYPE_NOT_SET
    }, ...Types.tracObjectTypes], [])


    // Is the attribute set up to use schemas
    const isUseSchema = Boolean(attribute.OBJECT_TYPES === "DATA" && attribute.BASIC_TYPE === trac.STRING && attribute.OPTIONS_VALUES === "USE_SCHEMA" && attribute.OPTIONS_LABELS === "USE_SCHEMA" && attribute.USE_OPTIONS)
    
    /**
     * A function that handles changes to the object types that the attribute can be set on. There is additional
     * logic to handle the 'ALL' case. We also need to deal with the case where an attribute was set for a
     * dataset only and then the user said to use the schema as the set of options. We now need to check if they
     * add any more object types and if they do remove any details from the using the schema.
     *
     * @param payload - The payload from the {@link SelectOption} component.
     */
    const handleObjectTypeChange = useCallback((payload: SelectOptionPayload<Option<string, trac.ObjectType>, true>): void => {

        const {id, value, isValid} = payload

        if (id == null) throw new Error(`id value in handleObjectTypeChange function is null`)

        let newValue: Partial<UiAttributesListRow>

        // If there is an ALL value or if all the non-ALL options have been selected then reset the value to ALL
        if (value.map(option => option.value).includes("ALL") || value.length === tracObjectTypesWithAll.length - 1) {

            newValue = {OBJECT_TYPES: "ALL"}

        } else {

            // Note that we map to the name of the object not its enum
            newValue = {OBJECT_TYPES: value.map(option => option.value).join("||")}
        }
       
        // Update the selection and the validation
        setAttribute((prevState) => {
            
            if (prevState === null) return null
            
            if (isUseSchema) {
                // When USE_OPTIONS is on the OPTIONS_VALUES and OPTIONS_LABELS are set to "" which is a single
                // blank option, this is not a valid option
                newValue.OPTIONS_VALUES = ""
                newValue.OPTIONS_LABELS = ""
                newValue.DEFAULT_VALUE = null
            }
            
            return {...prevState, ...newValue}
        })
        
        setValidation((prevState) => {

            // blank valid options are invalid but a null default value is valid for options
            const useSchemaValidation = isUseSchema ? {OPTIONS_VALUES: false, OPTIONS_LABELS: false, DEFAULT_VALUE: true} : {}
            
            return {...prevState, [id]: isValid, ...useSchemaValidation }
        })

    }, [isUseSchema, setAttribute, setValidation, tracObjectTypesWithAll.length])

    /**
     * A function that handles changes to the attribute ID. There is additional
     * logic to handle spaces as we do not allow them.
     * @param payload - The object of information from the SelectOption component.
     */
    const handleIdChange = useCallback((payload: SelectValuePayload): void => {

        const {id, value, isValid} = payload

        if (id !== "ID" || typeof value === "number") return

        // Update the value and the validation
        setAttribute((prevState) => (prevState ? {
            ...prevState,
            // The ID can can not have spaces
            [id]: value != null ? value.replace(new RegExp(/\s+/g, 'g'), "_") : value
        } : null))

        setValidation((prevState) => ({...prevState, [id]: isValid}))

    }, [setAttribute, setValidation])

    /**
     * A function that handles changes to the category and width multiplier.
     * @param payload - The payload from the {@link SelectOption} component.
     */
    const handleOptionChange = useCallback((payload: SelectOptionPayload<Option<string>, false>): void => {

        const {id, value, isValid} = payload

        // Typescript checking
        if (id === null || !includes(["CATEGORY", "WIDTH_MULTIPLIER"], id)) return

        // Get the value from the selected option
        let newValue = {[id]: value == null ? null : value.value}

        // Update the selection and the validation
        setAttribute((prevState) => (prevState ? {...prevState, ...newValue} : null))
        setValidation((prevState) => ({...prevState, [id]: isValid}))

    }, [setAttribute, setValidation])

    /**
     * A function that handles changes to the other attribute properties in this component that do not require
     * any special processing.
     *
     * @param payload - The payload from the {@link SelectValue} component.
     */
    const handleGeneralChange = useCallback((payload: SelectValuePayload): void => {

        const {id, value, isValid} = payload

        // Typescript checking
        if (id === null || !includes(["DESCRIPTION", "NAME", "TOOLTIP"], id)) return

        // Update the value and the validation
        setAttribute((prevState) => (prevState ? {...prevState, [id]: value} : null))
        setValidation((prevState) => ({...prevState, [id]: isValid}))

    }, [setAttribute, setValidation])

    // IDs need to be unique, so here we get the whole dataset of values for ID and extract the ID values. We then
    // remove the row being edited. Then, if the user set ID is in the resulting array we know that it is a duplicate
    // isDefined removes null
    const uniqueIds = useMemo(() => data.filter((row, i) => i !== index).map(row => row.ID).filter(isDefined).map(id => id.toUpperCase()), [data, index])

    /**
     * A function that is passed to a {@link SelectValue} component to use to determine if the value of ID
     * is valid, this replaces the default validity checker. ID values must be unique.
     */
    const checkIdValidity = useCallback((payload: SelectValueCheckValidityArgs): CheckValidityReturn => {

        const {
            maximumValue,
            minimumValue,
            value
        } = payload

        if (value != null && typeof value !== "string") {
            throw new Error(`SelectValue checkValidity function returned a non-string value for a string value`)
        } else if (value !== null && !isAttributeNameValid(value).isValid) {
            // This is a bespoke function for testing attribute names
            return {isValid: false, message: isAttributeNameValid(value).errors[0]}
        } else if (!value && minimumValue == null && maximumValue == null) {
            return {isValid: false, message: `ID must not be blank`}
        } else if (value == null || (minimumValue != null && value.length < minimumValue)) {
            return {isValid: false, message: `ID must be at least ${minimumValue} characters`}
        } else if (maximumValue != null && value.length > maximumValue) {
            return {isValid: false, message: `ID can not be more than ${maximumValue} characters`}
        } else if (uniqueIds.includes(value.toUpperCase())) {
            return {isValid: false, message: `This ID already exists, duplicates are not allowed`}
        } else {
            return {isValid: true, message: ""}
        }

    }, [uniqueIds])


    return (

        <React.Fragment>
            <HeaderTitle type={"h4"} text={"GeneralChartConfig information"}/>

            {/* Rows are d-flex by default*/}
            <Row className={"align-items-end"}>
                <Col xs={12}>
                    {/*A multi select that allows the user to pick which TRAC object types the attribute related to*/}
                    {/*Dynamically set maximumSelectionsBeforeMessageOverride so that we never truncate the list of selected items with a count*/}
                    <SelectOption basicType={trac.STRING}
                                  isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                  id={"OBJECT_TYPES"}
                                  isDispatched={false}
                                  isMulti={true}
                                  labelText={labels.OBJECT_TYPES}
                                  maximumSelectionsBeforeMessageOverride={tracObjectTypesWithAll.length}
                                  mustValidate={true}
                                  onChange={handleObjectTypeChange}
                                  options={tracObjectTypesWithAll}
                                  showValidationMessage={true}
                                  validationChecked={validationChecked}
                                  value={tracObjectTypesWithAll.filter(objectType => convertAnythingToStringArray(attribute.OBJECT_TYPES, "||").includes(objectType.value))}
                    />
                </Col>

                <Col xs={6} sm={4} className={"pt-1 pt-sm-0"}>
                    <SelectValue basicType={trac.STRING}
                                 checkValidity={checkIdValidity}
                                 id={"ID"}
                                 isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                 isDispatched={false}
                                 labelText={labels.ID}
                                 minimumValue={3}
                                 mustValidate={true}
                                 onChange={handleIdChange}
                                 showValidationMessage={true}
                                 tooltip={"A key for the attribute"}
                                 validationChecked={validationChecked}
                                 value={attribute.ID}
                    />
                </Col>

                <Col xs={6} sm={4} className={"pt-1 pt-sm-0"}>
                    <SelectOption basicType={trac.STRING}
                                  id={"CATEGORY"}
                                  isCreatable={true}
                                  isDispatched={false}
                                  labelText={labels.CATEGORY}
                                  mustValidate={true}
                                  onChange={handleOptionChange}
                                  onCreateNewOption={addUserSetCategory}
                                  options={categoryOptions}
                                  showValidationMessage={true}
                                  tooltip={"You can add your own category by typing it into the box and hitting enter"}
                                  validationChecked={validationChecked}
                                  value={categoryOptions.find(option => option.value === attribute.CATEGORY)}
                    />
                </Col>

                <Col xs={6} sm={4} className={"pt-1 pt-sm-0"}>
                    <SelectOption basicType={trac.INTEGER}
                                  id={"WIDTH_MULTIPLIER"}
                                  isCreatable={false}
                                  isDispatched={false}
                                  labelText={labels.WIDTH_MULTIPLIER}
                                  mustValidate={false}
                                  onChange={handleOptionChange}
                                  options={widthMultiplierOptions}
                                  showValidationMessage={true}
                                  validationChecked={validationChecked}
                                  value={widthMultiplierOptions.find(option => option.value === attribute.WIDTH_MULTIPLIER)}
                    />
                </Col>

                <Col xs={12} sm={9} className={"pt-1 pt-sm-0"}>
                    <SelectValue basicType={trac.STRING}
                                 id={"NAME"}
                                 isDispatched={false}
                                 labelText={labels.NAME}
                                 minimumValue={3}
                                 mustValidate={true}
                                 onChange={handleGeneralChange}
                                 showValidationMessage={true}
                                 tooltip={"An '%object%' string in the name will be converted to the object type when being set e.g. 'data'"}
                                 validationChecked={validationChecked}
                                 value={attribute.NAME}
                    />
                </Col>

                <Col xs={12} className={"pt-1 pt-sm-0"}>
                    <SelectValue basicType={trac.STRING}
                                 id={"DESCRIPTION"}
                                 isDispatched={false}
                                 labelText={labels.DESCRIPTION}
                                 minimumValue={20}
                                 mustValidate={true}
                                 onChange={handleGeneralChange}
                                 rows={4}
                                 showValidationMessage={true}
                                 specialType={"TEXTAREA"}
                                 tooltip={"An '%object%' string in the description will be converted to the object type when being set e.g. 'data'"}
                                 validationChecked={validationChecked}
                                 value={attribute.DESCRIPTION}
                    />
                </Col>

                <Col xs={12} className={"pt-1 pt-sm-0"}>
                    <SelectValue basicType={trac.STRING}
                                 id={"TOOLTIP"}
                                 isDispatched={false}
                                 labelText={labels.TOOLTIP}
                                 mustValidate={false}
                                 onChange={handleGeneralChange}
                                 rows={4}
                                 showValidationMessage={true}
                                 specialType={"TEXTAREA"}
                                 tooltip={"This should contain additional information for the user, it will be shown when they hover over an '?' icon. An '%object%' string in the tooltip will be converted to the object type when being set e.g. 'data'"}
                                 validationChecked={validationChecked}
                                 value={attribute.TOOLTIP}
                    />
                </Col>
            </Row>
        </React.Fragment>
    )
};

ConfigureAttributeGeneral.propTypes = {

    attribute: PropTypes.object.isRequired,
    labels: PropTypes.object.isRequired,
    setAttribute: PropTypes.func.isRequired,
    setValidation: PropTypes.func.isRequired,
    validationChecked: PropTypes.bool.isRequired
};