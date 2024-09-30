import {BooleanOptions, DateFormats} from "../../../../config/config_general";
import Col from "react-bootstrap/Col";
import {convertIsoDateStringToFormatCode} from "../../../utils/utils_formats";
import {convertObjectKeysToOptions} from "../../../utils/utils_object";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {includes, isDateFormat, isDefined, isTracBoolean, isTracDateOrDatetime, isTracNumber, isTracString} from "../../../utils/utils_trac_type_chckers";
import {convertStringToInteger, isValidIsoDatetimeString} from "../../../utils/utils_string";
import {Option, SelectOptionPayload} from "../../../../types/types_general";
import PropTypes from "prop-types";
import React, {useCallback, useMemo} from "react";
import Row from "react-bootstrap/Row";
import {SelectOption} from "../../../components/SelectOption";
import {SelectToggle} from "../../../components/SelectToggle";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {Types} from "../../../../config/config_trac_classifications";
import {UiAttributesListRow} from "../../../../types/types_attributes_and_parameters";
import {convertArrayToOptions} from "../../../utils/utils_arrays";

// A set of options for the number of rows selector, only for string non-option attributes
const numberOfRowOptions = convertArrayToOptions([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

/**
 * This component shows the 'Type and format' section of the ConfigureAttributeModal component, this allows the user to
 * edit an attribute's basic type (e.g. string, boolean etc.) and how it should be set, for example using options.
 */

type Props = {

    /**
     * The attribute being edited.
     */
    attribute: UiAttributesListRow
    /**
     * Labels for each form element based on the fieldLabel entries in the dataset schema.
     */
    labels: Partial<Record<string, string>>
    /**
     * A hook to set the state in the parent component.
     */
    setAttribute: React.Dispatch<React.SetStateAction<null | UiAttributesListRow>>
    /**
     * A hook to set the state in the parent component.
     */
    setValidation: React.Dispatch<React.SetStateAction<Partial<Record<keyof UiAttributesListRow, boolean>>>>
    /**
     * Whether to form has been validated and then show any in valid messages.
     */
    validationChecked: boolean
};

const ConfigureAttributeType = (props: Props) => {

    const {attribute, validationChecked, labels, setAttribute, setValidation} = props

    /**
     * A set of options for all the date format options.
     */
    const dateFormatOptions = useMemo(() => convertObjectKeysToOptions(DateFormats, true), [])

    /**
     * A function that handles changes to the attribute data type e.g. INTEGER, STRING etc. There is additional
     * logic to handle how changing this needs to reset other attribute values.
     *
     * @param payload - The payload from the {@link SelectOption} component.
     */
    const handleBasicTypeChange = useCallback((payload: SelectOptionPayload<Option<string, trac.BasicType>, false>): void => {

        const {id, value, isValid} = payload

        if (id == null) return

        // booleanOptionStringsAsArray should be ["TRUE", "FALSE"] but we get it directly from the config in case it gets updated there
        const booleanOptionStringsAsArray = BooleanOptions.map(option => option.details.asString).filter(isDefined)

        // A quicker type checker that we are going to use to see if we can keep the current
        // DEFAULT_VALUE, MINIMUM_VALUE & MAXIMUM_VALUE values when the type changes
        // We can do this because we know that the default value is always a valid string version for the
        // type so "123" for an integer.
        const checkTypeConversion = (basicType: trac.BasicType, value: string | null | undefined): string | null => {

            if (value == null) {

                return null

            } else if (isTracString(basicType)) {

                // When converting to a string the value is always valid
                // min and max should be integers as strings for string values
                return value

            } else if (isTracNumber(basicType)) {

                // convertStringToInteger returns NaN if there is no valid integer
                const newNumber = basicType === trac.INTEGER ? convertStringToInteger(value) : parseFloat(value)

                // When converting to a number the existing DEFAULT_VALUE is valid is it is a number
                return isNaN(newNumber) ? null : newNumber.toString()

            } else if (isTracBoolean(basicType) && booleanOptionStringsAsArray.includes(value)) {

                // When converting to a boolean the existing DEFAULT_VALUE is valid if a boolean string
                return value


            } else if (isTracDateOrDatetime(basicType) && isValidIsoDatetimeString(value)) {

                // When converting to a boolean the existing DEFAULT_VALUE is valid if a boolean string
                return value

            } else {

                return null
            }
        }

        // These are the two objects that we are going to create that contain the required changes
        let newValue: Partial<UiAttributesListRow> = {}

        let newValidation: Partial<Record<keyof UiAttributesListRow, boolean>> = {
            BASIC_TYPE: isValid,
            DEFAULT_VALUE: true,
            OPTIONS_VALUES: true,
            OPTIONS_LABELS: true,
            MINIMUM_VALUE: true,
            MAXIMUM_VALUE: true
        }

        // Note that all the update logic is inside a useState function, this allows us to access the existing 
        // attribute values. 
        setAttribute((prevState) => {

            if (prevState == null) return null

            if (value) {

                newValue.BASIC_TYPE = value.type

                // Check some other properties to see if the type change still allows the existing value to be retained,
                // rest them to null if they can't be
                newValue.DEFAULT_VALUE = checkTypeConversion(value.type, prevState.DEFAULT_VALUE)
                // For all variable types the MINIMUM_VALUE and MAXIMUM_VALUE types match the DEFAULT_VALUE, except for
                // strings where they are string lengths and therefore integers
                newValue.MINIMUM_VALUE = checkTypeConversion(isTracString(value.type) ? trac.INTEGER : value.type, prevState.MINIMUM_VALUE)
                newValue.MAXIMUM_VALUE = checkTypeConversion(isTracString(value.type) ? trac.INTEGER : value.type, prevState.MAXIMUM_VALUE)

                // Convert the options values/labels into arrays
                let newOptionValues = prevState.OPTIONS_VALUES == null ? [] : prevState.OPTIONS_VALUES.split("||")
                let newOptionLabels = prevState.OPTIONS_LABELS == null ? [] : prevState.OPTIONS_LABELS.split("||")

                // Check if any of the values are no longer valid given the new type of variable, if they are invalid
                // remove the value and the corresponding label
                let remappedNewOptionValues = newOptionValues.map(item => checkTypeConversion(value.type, item)).filter((option, i) => {

                    const isNull = option === null

                    if (isNull) newOptionLabels.splice(i, 1)

                    return !isNull
                })

                // Zip the values and labels back up
                newValue.OPTIONS_VALUES = remappedNewOptionValues.join("||")
                newValue.OPTIONS_LABELS = newOptionLabels.join("||")

                // booleans can't have user set options or multiple values
                if (value.type === trac.BOOLEAN) {
                    newValue.USE_OPTIONS = false
                    newValue.IS_MULTI = false
                }

                // The format codes between numbers are interchangeable, otherwise we need to null it
                if (!(isTracNumber(prevState.BASIC_TYPE) && isTracNumber(newValue.BASIC_TYPE))) {
                    newValue.FORMAT_CODE = null
                }

            } else {

                newValue.BASIC_TYPE = null
                newValue.DEFAULT_VALUE = null
                newValue.MINIMUM_VALUE = null
                newValue.MAXIMUM_VALUE = null
                newValue.OPTIONS_VALUES = null
                newValue.OPTIONS_LABELS = null
                newValue.FORMAT_CODE = null
            }

            // Not only do we need to update the validation for the item that changed but because
            // we override other properties to null at the same time we also need to update the validation
            // of those other properties to what they should be when set to null.

            // The attribute values are needed to set the validation state

            // The format codes between numbers are interchangeable, otherwise we need to null it
            if ((isTracNumber(prevState.BASIC_TYPE) && isTracNumber(newValue.BASIC_TYPE))) {
                newValidation.FORMAT_CODE = true
            }

            // The null override to FORMAT_CODE is invalid for these data types
            if (isTracDateOrDatetime(newValue.BASIC_TYPE)) {
                newValidation.FORMAT_CODE = false
            }

            // The null override to DEFAULT_VALUE is invalid when hidden and DEFAULT_VALUE is empty
            if ((prevState.HIDDEN && !prevState.DEFAULT_VALUE)) newValidation.DEFAULT_VALUE = false

            return {...prevState, ...newValue}
        })

        // Not only do we need to update the validation for the item that changed but because
        // we override other properties to null at the same time we also need to update the validation
        // of those other properties to what they should be when set to null.

        // Note that when changing basic type this will unmount the ConfigureAttributeOptions SelectValue
        // components and remount new ones for the new basic type. This means that the validation of the
        // options will be reset after this.
        setValidation((prevState) => ({...prevState, ...newValidation}))

    }, [setAttribute, setValidation])

    /**
     * A function that handles changes to whether the attribute values should be set as a value or
     * from a list of options. There is additional logic to handle when turning this to false.
     * @param payload - The object of information from the SelectToggle component.
     */
    const handleUseOptionsChange = useCallback((payload: { id: "USE_OPTIONS", isValid: boolean, value: boolean | null, basicType: trac.BasicType }): void => {

        const {basicType, id, value, isValid} = payload

        // These are the two objects that we are going to create that contain the required changes
        let newValue: Partial<UiAttributesListRow> = {[id]: value || false, DEFAULT_VALUE: null}

        let newValidation: Partial<Record<keyof UiAttributesListRow, boolean>> = {
            [id]: isValid,
            FORMAT_CODE: true
        }

        // Note that all the update logic is inside a useState function, this allows us to access the existing 
        // attribute values. 
        setAttribute((prevState) => {

            if (prevState == null) return null

            if (value !== true) {

                // When USE_OPTIONS is turned off the OPTIONS_VALUES and OPTIONS_LABELS are reset
                newValue.OPTIONS_VALUES = null
                newValue.OPTIONS_LABELS = null

            } else if (value) {

                // When USE_OPTIONS is turned on the OPTIONS_VALUES and OPTIONS_LABELS are set to "" which is a single
                // blank option
                newValue.OPTIONS_VALUES = ""
                newValue.OPTIONS_LABELS = ""

                // When the user turns USE_OPTIONS on we do not allow validation, we assume that the
                // user sets valid options
                newValue.MUST_VALIDATE = false
                newValue.MINIMUM_VALUE = null
                newValue.MAXIMUM_VALUE = null

                // For dates, we retain any format set, so we can use to format the options
                // For non-dates we say that the labels set by the user will have the formats applied
                if (!isTracDateOrDatetime(basicType)) {
                    newValue.FORMAT_CODE = null
                }
            }

            // The attribute values are needed to set the validation state

            // When USE_OPTIONS is turned on a null FORMAT_CODE is always valid when not a date
            if (value === true && !isTracDateOrDatetime(basicType)) newValidation.FORMAT_CODE = true

            // When changing USE_OPTIONS the default value is set to null, so it is always valid unless hidden
            newValidation.DEFAULT_VALUE = !prevState.HIDDEN

            if (value !== true) {

                // When USE_OPTIONS is turned off the OPTIONS_VALUES and OPTIONS_LABELS are reset, this is valid
                newValidation.OPTIONS_VALUES = true
                newValidation.OPTIONS_LABELS = true

            } else if (value) {
                // When the user turns USE_OPTIONS on we do not allow validation, we assume that the
                // user sets valid options, so min and max values are valid
                newValidation.MINIMUM_VALUE = true
                newValidation.MAXIMUM_VALUE = true
            }

            return {...prevState, ...newValue}
        })

        // Not only do we need to update the validation for the item that changed but because
        // we override other properties to null at the same time we also need to update the validation
        // of those other properties to what they should be when set to null.
        setValidation((prevState) => ({...prevState, ...newValidation}))

    }, [setAttribute, setValidation])

    /**
     * A function that handles changes to whether the user sets the attribute to be multiple valued.
     * There is additional logic to handle when turning this to false.
     * @param payload - The object of information from the SelectToggle component.
     */
    const handleIsMultiChange = useCallback((payload: { id: "IS_MULTI", isValid: boolean, value: boolean | null }): void => {

        const {id, value} = payload

        // Note that all the update logic is inside a useState function, this allows us to access the existing 
        // attribute values. 
        setAttribute((prevState) => {

            if (prevState == null) return null

            // When moving from multi to single options we need to truncate the DEFAULT_VALUE string to the first option
            let newDefaultValue = prevState.DEFAULT_VALUE == null ? [] : prevState.DEFAULT_VALUE.split("||")

            // When the attribute is a number we inject "Not a number" if the user adds an option as a string that is
            // not a valid number. We need to check here that if we truncate the options we pick a valid number option
            if (isTracNumber(prevState.BASIC_TYPE)) {
                newDefaultValue = newDefaultValue.filter(value => value === "" || !isNaN(Number(value)))
            }

            // When the attribute is a date we inject "Not a date" if the user adds an option as a string that is
            // not a valid date. We need to check here that if we truncate the options we pick a valid number option
            if (isTracNumber(prevState.BASIC_TYPE)) {
                newDefaultValue = newDefaultValue.filter(value => isValidIsoDatetimeString(value))
            }

            let newValue: Partial<UiAttributesListRow> = {
                [id]: value || false,
                DEFAULT_VALUE: value !== true && newDefaultValue.length > 0 ? newDefaultValue[0] : prevState.DEFAULT_VALUE,
            }

            if (value) {

                // When the user turns IS_MULTI on we do not allow validation, we assume that the
                // user sets valid options
                newValue.MUST_VALIDATE = false
                newValue.MINIMUM_VALUE = null
                newValue.MAXIMUM_VALUE = null
            }

            return {...prevState, ...newValue}

        })

        // We do not need to set the validation of DEFAULT_VALUE as we assume all values pre change were valid
        setValidation((prevState) => {

            let newValidation: Partial<Record<keyof UiAttributesListRow, boolean>> = {}

            if (value) {
                // When the user turns USE_OPTIONS on we do not allow validation, we assume that the
                // user sets valid options, so min and max values are valid
                newValidation.MINIMUM_VALUE = true
                newValidation.MAXIMUM_VALUE = true
            }

            return {...prevState, ...newValidation}
        })

    }, [setAttribute, setValidation])

    /**
     * A function that handles changes to the date format to apply. There is additional logic to handle when
     * the user is setting their own options and the labels need to be reformatted.
     * @param payload - The object of information from the SelectOption component.
     */
    const handleDateFormatCodeChange = useCallback((payload: { id: "FORMAT_CODE", isValid: boolean, value: null | Option<string> }): void => {

        const {id, value, isValid} = payload

        // Cope with the fact that FORMAT_CODE could be set via an input or an option
        let newValue: Partial<UiAttributesListRow> = {[id]: value == null ? null : value.value}

        // Note that all the update logic is inside a useState function, this allows us to access the existing
        // attribute values.
        setAttribute((prevState) => {

            if (prevState == null) return null

            // If the date format to apply has changed then the option labels need to be recast to
            // the new format if USE_OPTIONS is set to true

            // Convert the string stored version of the values or labels into an array
            const newValueOptions = !prevState.OPTIONS_VALUES ? [] : prevState.OPTIONS_VALUES.split("||")

            newValue.OPTIONS_LABELS = newValueOptions.map(option => option && isDateFormat(newValue.FORMAT_CODE) ? convertIsoDateStringToFormatCode(option, newValue.FORMAT_CODE) : "").join("||")

            return {...prevState, ...newValue}
        })

        setValidation((prevState) => ({...prevState, [id]: isValid}))

    }, [setAttribute, setValidation])


    /**
     * A function that handles changes to the number of rows.
     * @param payload - The payload from the {@link SelectOption} component.
     */
    const handleNumberRowsOptionChange = useCallback((payload: SelectOptionPayload<Option<string>, false>): void => {

        const {id, value, isValid} = payload

        // Typescript checking
        if (id === null || !includes(["NUMBER_OF_ROWS"], id)) return

        // Get the value from the selected option
        let newValue = {[id]: value == null ? null : value.value}

        // Update the selection and the validation
        setAttribute((prevState) => (prevState ? {...prevState, ...newValue} : null))
        setValidation((prevState) => ({...prevState, [id]: isValid}))

    }, [setAttribute, setValidation])

    return (

        <React.Fragment>

            <HeaderTitle type={"h4"} text={"Data type & format"}/>

            {/*Rows are d-flex by default*/}
            <Row className={" align-items-end"}>
                <Col xs={12} lg={4}>
                    {/*flex-fill means we can push the widgets to the bottom so alignment is better*/}
                    <SelectOption basicType={trac.INTEGER}
                                  id={"BASIC_TYPE"}
                                  isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                  isDispatched={false}
                                  labelText={labels.BASIC_TYPE}
                                  mustValidate={true}
                                  onChange={handleBasicTypeChange}
                                  options={Types.tracBasicTypes}
                                  showValidationMessage={true}
                                  validationChecked={validationChecked}
                                  value={Types.tracBasicTypes.find(option => option.type === attribute.BASIC_TYPE)}
                    />
                </Col>

                {/*Booleans don't have options that can be set by the user, the options are already set in the config*/}
                {/*Booleans can't be multivalued*/}
                {!isTracBoolean(attribute.BASIC_TYPE) &&
                    <React.Fragment>
                        <Col xs={6} lg={4}>
                            <SelectToggle className={"text-left text-sm-center"}
                                          id={"USE_OPTIONS"}
                                          isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                          isDispatched={false}
                                          labelPosition={"left"}
                                          labelText={`${labels.USE_OPTIONS}:`}
                                          onChange={handleUseOptionsChange}
                                          showValidationMessage={true}
                                          value={attribute.USE_OPTIONS}
                            />
                        </Col>

                        <Col xs={6} lg={4}>
                            <SelectToggle className={"text-left text-sm-center"}
                                          id={"IS_MULTI"}
                                          isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                          isDispatched={false}
                                          labelPosition={"left"}
                                          labelText={`Allow multiple ${attribute.USE_OPTIONS ? "options" : "values"} to be selected:`}
                                          onChange={handleIsMultiChange}
                                          showValidationMessage={true}
                                          value={attribute.IS_MULTI}
                            />
                        </Col>
                    </React.Fragment>
                }

                {/*Date and datetime formats are dealt with differently to numbers, there is a widget we use for
                numbers that is rendered in the parent component.*/}
                {attribute.BASIC_TYPE != null && isTracDateOrDatetime(attribute.BASIC_TYPE) &&
                    <Col xs={12} sm={6}>
                        <SelectOption basicType={trac.STRING}
                                      id={"FORMAT_CODE"}
                                      isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                      isDispatched={false}
                                      labelText={labels.FORMAT_CODE}
                                      onChange={handleDateFormatCodeChange}
                                      options={dateFormatOptions}
                                      mustValidate={true}
                                      showValidationMessage={true}
                                      validationChecked={validationChecked}
                                      value={dateFormatOptions.find(option => option.value === attribute.FORMAT_CODE)}
                        />
                    </Col>
                }
                    {/*strings and options are allowed a wider input to use, dates align with the date format box*/}
                        {isTracString(attribute.BASIC_TYPE) && !attribute.USE_OPTIONS &&
                            <Col xs={12} sm={6} lg={4}>

                                <SelectOption basicType={trac.INTEGER}
                                              id={"NUMBER_OF_ROWS"}
                                              isCreatable={false}
                                              isDispatched={false}
                                              labelText={"Number of rows of text"}
                                              mustValidate={false}
                                              onChange={handleNumberRowsOptionChange}
                                              options={numberOfRowOptions}
                                              showValidationMessage={true}
                                              tooltip={"When set to one this attribute will be a single input, when two or more it will be a larger text area."}
                                              validationChecked={validationChecked}
                                              value={numberOfRowOptions.find(option => option.value === attribute.NUMBER_OF_ROWS)}
                                />
                            </Col>
                        }
            </Row>
        </React.Fragment>

    )
};

ConfigureAttributeType.propTypes = {

    attribute: PropTypes.object.isRequired,
    labels: PropTypes.object.isRequired,
    setAttribute: PropTypes.func.isRequired,
    setValidation: PropTypes.func.isRequired,
    validationChecked: PropTypes.bool.isRequired
};

export default ConfigureAttributeType;