/**
 * A component that shows the 'Default value' section of the {@link ConfigureAttributeModal} component, this allows
 * the user to edit an attribute's default value.
 * @module
 * @category Component
 */

import {Option, SelectPayload} from "../../../../types/types_general";
import {BooleanOptions} from "../../../../config/config_general";
import Col from "react-bootstrap/Col";
import {convertArrayToOptions} from "../../../utils/utils_arrays";
import {isAsStringOption, isDateFormat, isMultiOption, isOption, isTracBoolean, isTracDateOrDatetime, isTracNumber, isTracString} from "../../../utils/utils_trac_type_chckers";
import {isValidIsoDatetimeString} from "../../../utils/utils_string";
import {HeaderTitle} from "../../../components/HeaderTitle";
import PropTypes from "prop-types";
import React, {useCallback, useMemo} from "react";
import Row from "react-bootstrap/Row";
import {SelectDate} from "../../../components/SelectDate";
import {SelectOption} from "../../../components/SelectOption";
import {SelectValue} from "../../../components/SelectValue";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {UiAttributesListRow} from "../../../../types/types_attributes_and_parameters";

/**
 * An interface for the props of the ConfigureAttributeDefaultValue component.
 */
interface Props {

    /**
     * The attribute being edited.
     */
    attribute: UiAttributesListRow
    /**
     * A hook to set the state in the parent component.
     */
    setAttribute: React.Dispatch<React.SetStateAction<null | UiAttributesListRow>>
    /**
     * A hook to set the state in the parent component.
     */
    setValidation: React.Dispatch<React.SetStateAction<Partial<Record<keyof UiAttributesListRow, boolean>>>>
    /**
     * Whether to form has been validated and then show any in validation messages.
     */
    validationChecked: boolean
}

export const ConfigureAttributeDefaultValue = (props: Props) => {

    const {attribute, validationChecked, setAttribute, setValidation} = props

    /**
     * A function that handles changes to the default value, there is some complex logic based on the type of attribute
     * (e.g. boolean or string etc.) and the method for setting the value (e.g. by option).
     *
     * @param payload - The object of information from the SelectValue or SelectDate components.
     */
    const handleDefaultValueChange = useCallback((payload: SelectPayload<Option, boolean>): void => {

        const {id, value, isValid} = payload

        if (id !== "DEFAULT_VALUE") return

        // These are the two objects that we are going to create that contain the required changes
        let newValue: Partial<UiAttributesListRow> = {}
        let newValidation: Partial<Record<keyof UiAttributesListRow, boolean>> = {}

        // Note that all the update logic is inside a useState function, this allows us to access the existing
        // attribute values and also use the attribute values to set the validation state
        setAttribute((prevState) => {

            if (prevState == null) return null

            // DEFAULT_VALUE here is for booleans as value is the object for the option
            if (isTracBoolean(prevState.BASIC_TYPE)) {

                if (!isOption(value)) throw new TypeError(`The default value is not an option, this can not be used for boolean attributes`)

                // DEFAULT_VALUE is stored as a string, so we need to convert boolean to a string when saving, this is the
                // asString property in the option.
                newValue = {[id]: isAsStringOption(value) ? value.details.asString : null}
                newValidation = {[id]: isValid}
            }
            // DEFAULT_VALUE here is for non-booleans when USE_OPTIONS is false. In this case value is not an object or an array
            else if (!isTracBoolean(prevState.BASIC_TYPE) && !prevState.USE_OPTIONS && !prevState.IS_MULTI) {

                if (isMultiOption(value)) throw new TypeError(`The default value is an array of options, this can not be used for non-boolean attributes when USE_OPTIONS=false`)
                if (isOption(value)) throw new TypeError(`The default value is an option, this can not be used for non-boolean attributes when USE_OPTIONS=false`)

                newValue = {[id]: value == null ? null : value.toString()}

                newValidation = {[id]: isValid}
            }
                // DEFAULT_VALUE here is for non-booleans or when USE_OPTIONS is true as value is the object for the option or
            // an array of options if IS_MULTI is true
            else if (!isTracBoolean(prevState.BASIC_TYPE) && (prevState.USE_OPTIONS || prevState.IS_MULTI)) {

                if (!(value == null || isMultiOption(value) || isOption(value))) throw new TypeError(`The default value is not an option or array of options, this can not be used for non-boolean attributes when USE_OPTIONS=true`)

                const nanText = "Not a number"
                const nadText = "Not a date"

                // This is just a quick function that converts the option value to a string, it first checks that
                // the entered string from the user is a valid number or date before returning a string.
                const checkUserEntries = (option: Option | null): string => {

                    if (option == null || option.value == null) {
                        return ""
                    } else if (isTracNumber(prevState.BASIC_TYPE)) {
                        return option.value === "" || isNaN(Number(option.value)) ? nanText : Number(option.value).toString()
                    } else if (isTracDateOrDatetime(prevState.BASIC_TYPE)) {
                        return isValidIsoDatetimeString(option.value.toString()) ? option.value.toString() : nadText
                    } else {
                        return option.value.toString()
                    }
                }

                // If the value is an array it is a multi select, so we need to concat the values together
                if (value && isMultiOption(value)) {

                    // We need to check that numbers are valid numbers
                    let newValues = value.map(option => checkUserEntries(option))

                    // This next bit is a little of React housekeeping. We have an array of strings entered by the
                    // user and any of which the values can be converted to NaN. React complains if the strings are duplicated
                    // since they are used as keys. So here we see if there is a Nan and if so remove them and add one at the
                    // end - to the user it looks like the one they just added was converted to NaN.
                    const hasNan = newValues.findIndex(value => value === nanText)

                    // Only redo the ordering and filtering when it is the latest entry that is the second NaN
                    if (hasNan > -1 && hasNan < newValues.length - 1 && newValues[newValues.length - 1] === nanText) {
                        newValues = newValues.filter(value => value !== nanText)
                        newValues.push(nanText)
                    }

                    newValue = {[id]: newValues.join("||")}

                } else {
                    // It's a single option
                    newValue = {[id]: checkUserEntries(value)}
                }

                newValidation = {[id]: isValid}
            }

            return {...prevState, ...newValue}
        })

        setValidation((prevState) => ({...prevState, ...newValidation}))

    }, [setAttribute, setValidation])

    /**
     * An array containing a set of options for the default value, this is used when USE_OPTIONS is true and the
     * user has set a range of options that can be picked as the default value.
     */
    const options = useMemo((): Option<null | string | number>[] => {

        // Convert the string stored version of the values or labels into an array
        const newValueOptions = !attribute.OPTIONS_VALUES ? [] : attribute.OPTIONS_VALUES.split("||")
        const newLabelOptions = !attribute.OPTIONS_LABELS ? [] : attribute.OPTIONS_LABELS.split("||")

        // Build the array of options
        return newValueOptions.map((value, i) => {
            return {
                value: !value ? null : isTracNumber(attribute.BASIC_TYPE) ? parseFloat(value) : value,
                label: newLabelOptions[i] || "No label set"
            }
        })

    }, [attribute.BASIC_TYPE, attribute.OPTIONS_LABELS, attribute.OPTIONS_VALUES])

    return (
        <React.Fragment>

            {attribute.ID !== "business_segments" &&
                <React.Fragment>
                    <HeaderTitle type={"h4"}
                                 text={`Default ${attribute.USE_OPTIONS ? "option" : "value"} (optional)`}
                                 tooltip={attribute.USE_OPTIONS || attribute.IS_MULTI ? "Press enter or tab to add an option, dates should be entered in an ISO format e.g. 2019-04-23" : undefined}
                    />

                    <Row>
                        {/*strings and options are allowed a wider input to use, dates align with the date format box*/}
                        <Col xs={12}
                             md={isTracString(attribute.BASIC_TYPE) || attribute.IS_MULTI ? 8 : isTracDateOrDatetime(attribute.BASIC_TYPE) ? 6 : 6}>

                            {/*We have different components based on the type of attribute*/}
                            {/*maximumValue & minimumValue allow us to validate the input*/}
                            {isTracNumber(attribute.BASIC_TYPE) && !attribute.USE_OPTIONS && !attribute.IS_MULTI &&
                                <SelectValue basicType={attribute.BASIC_TYPE}
                                             id={"DEFAULT_VALUE"}
                                             isDispatched={false}
                                             maximumValue={attribute.MUST_VALIDATE && attribute.MAXIMUM_VALUE ? parseFloat(attribute.MAXIMUM_VALUE) : undefined}
                                             minimumValue={attribute.MUST_VALIDATE && attribute.MINIMUM_VALUE ? parseFloat(attribute.MINIMUM_VALUE) : undefined}
                                             mustValidate={Boolean((attribute.DEFAULT_VALUE && attribute.MUST_VALIDATE))}
                                             onChange={handleDefaultValueChange}
                                             showValidationMessage={true}
                                             validationChecked={validationChecked}
                                             value={attribute.DEFAULT_VALUE == null ? null : parseFloat(attribute.DEFAULT_VALUE)}
                                />
                            }

                            {/*maximumValue & minimumValue allow us to validate the input*/}
                            {isTracString(attribute.BASIC_TYPE) && !attribute.USE_OPTIONS && !attribute.IS_MULTI &&
                                <SelectValue basicType={attribute.BASIC_TYPE}
                                             id={"DEFAULT_VALUE"}
                                             isDispatched={false}
                                             maximumValue={attribute.MUST_VALIDATE && attribute.MAXIMUM_VALUE ? parseFloat(attribute.MAXIMUM_VALUE) : undefined}
                                             minimumValue={attribute.MUST_VALIDATE && attribute.MINIMUM_VALUE ? parseFloat(attribute.MINIMUM_VALUE) : undefined}
                                             mustValidate={Boolean((attribute.DEFAULT_VALUE && attribute.MUST_VALIDATE))}
                                             onChange={handleDefaultValueChange}
                                             showValidationMessage={true}
                                             validationChecked={validationChecked}
                                             value={attribute.DEFAULT_VALUE}
                                />
                            }

                            {/*maximumValue & minimumValue allow us to validate the input*/}
                            {isTracDateOrDatetime(attribute.BASIC_TYPE) && !attribute.USE_OPTIONS && !attribute.IS_MULTI &&
                                <SelectDate basicType={attribute.BASIC_TYPE}
                                            formatCode={attribute.FORMAT_CODE == null || !isDateFormat(attribute.FORMAT_CODE) ? null : attribute.FORMAT_CODE}
                                            id={"DEFAULT_VALUE"}
                                            isClearable={true}
                                            isDispatched={false}
                                            maximumValue={attribute.MUST_VALIDATE ? attribute.MAXIMUM_VALUE : undefined}
                                            minimumValue={attribute.MUST_VALIDATE ? attribute.MINIMUM_VALUE : undefined}
                                            mustValidate={Boolean((attribute.DEFAULT_VALUE != null && attribute.MUST_VALIDATE))}
                                            onChange={handleDefaultValueChange}
                                            showValidationMessage={true}
                                            validationChecked={validationChecked}
                                            value={attribute.DEFAULT_VALUE}
                                />
                            }

                            {/*Setting default boolean values use a dropdown rather than a toggle*/}
                            {/*You have to pick one as there is a none option available*/}
                            {isTracBoolean(attribute.BASIC_TYPE) &&
                                <SelectOption basicType={trac.BOOLEAN}
                                              id={"DEFAULT_VALUE"}
                                              isDispatched={false}
                                              mustValidate={false}
                                              onChange={handleDefaultValueChange}
                                              options={BooleanOptions}
                                              showValidationMessage={true}
                                              validationChecked={validationChecked}
                                              value={BooleanOptions.find((option) => option.details.asString === attribute.DEFAULT_VALUE)}
                                />
                            }

                            {/*Setting default values for non-booleans when USE_OPTIONS is true means you have to use a dropdown*/}
                            {(isTracNumber(attribute.BASIC_TYPE) || isTracDateOrDatetime(attribute.BASIC_TYPE) || isTracString(attribute.BASIC_TYPE)) && attribute.USE_OPTIONS &&
                                <SelectOption basicType={attribute.BASIC_TYPE}
                                              id={"DEFAULT_VALUE"}
                                              isClearable={true}
                                              isDispatched={false}
                                              isMulti={Boolean(attribute.IS_MULTI)}
                                              mustValidate={false}
                                              onChange={handleDefaultValueChange}
                                              options={options}
                                              showValidationMessage={true}
                                              validationChecked={validationChecked}
                                              value={Boolean(attribute.IS_MULTI) ? options.filter((option) => (!attribute.DEFAULT_VALUE ? [] : attribute.DEFAULT_VALUE.split("||")).includes(option.value ? option.value.toString() : "")) : options.find(option => (option.value ? option.value.toString() : null) === attribute.DEFAULT_VALUE)}
                                />
                            }

                            {/*Setting default values for non-booleans when USE_OPTIONS is false but IS_MULTI is true means you have to use a dropdown*/}
                            {(isTracNumber(attribute.BASIC_TYPE) || isTracDateOrDatetime(attribute.BASIC_TYPE) || isTracString(attribute.BASIC_TYPE)) && !attribute.USE_OPTIONS && attribute.IS_MULTI &&
                                <SelectOption basicType={attribute.BASIC_TYPE}
                                              id={"DEFAULT_VALUE"}
                                              isCreatable={true}
                                              isDispatched={false}
                                              isMulti={attribute.IS_MULTI}
                                              maximumSelectionsBeforeMessageOverride={10}
                                              mustValidate={false}
                                              onChange={handleDefaultValueChange}
                                              options={undefined}
                                              showValidationMessage={true}
                                              validationChecked={validationChecked}
                                              value={convertArrayToOptions(!attribute.DEFAULT_VALUE ? [] : attribute.DEFAULT_VALUE.split("||"), false)}
                                />
                            }
                        </Col>
                    </Row>
                </React.Fragment>
            }
        </React.Fragment>
    )
};

ConfigureAttributeDefaultValue.propTypes = {

    attribute: PropTypes.object.isRequired,
    setAttribute: PropTypes.func.isRequired,
    setValidation: PropTypes.func.isRequired,
    validationChecked: PropTypes.bool.isRequired
};