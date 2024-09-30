import {Alert} from "../../../components/Alert";
import {Button} from "../../../components/Button";
import {CheckValidityReturn, ConfirmButtonPayload, SelectDatePayload, SelectTogglePayload, SelectValueCheckValidityArgs, SelectValuePayload} from "../../../../types/types_general";
import Col from "react-bootstrap/Col";
import {ConfirmButton} from "../../../components/ConfirmButton";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {Icon} from "../../../components/Icon";
import {isDateFormat, isTracDateOrDatetime, isTracNumber, isTracString} from "../../../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React, {useCallback} from "react";
import Row from "react-bootstrap/Row";
import {SelectDate} from "../../../components/SelectDate";
import {SelectToggle} from "../../../components/SelectToggle";
import {SelectValue} from "../../../components/SelectValue";
import {showToast} from "../../../utils/utils_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {UiAttributesListRow} from "../../../../types/types_attributes_and_parameters";

// Some Bootstrap grid layouts
const lgGrid = {span: 4, offset: 2}
const mdGrid = {span: 6, offset: 0}
const xsGrid = {span: 5, offset: 0}

// A dummy function, used when no onChange function is needed
const dummyFunction = () => null

/**
 * This component shows the 'Options' section of the ConfigureAttributeModal component, this allows the user to define a
 * set of options that the user can pick from when setting the attribute. Booleans are not allowed to have options
 * set as there is already a list defined for these in the config.
 */

type Props = {

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
     * Whether to form has been validated and then show any in valid messages.
     */
    validationChecked: boolean
};

const ConfigureAttributeOptions = (props: Props) => {

    const {attribute, validationChecked, setAttribute, setValidation} = props

    /**
     * A function that adds a new option to the list of those defined by the user. We prevent additional options
     * from being added if there are already 20.
     */
    const handleAddOption = (): void => {

        const maximumOptions = 100

        if ((attribute.OPTIONS_VALUES == null ? [] : attribute.OPTIONS_VALUES.split("||")).length < maximumOptions) {

            // A double pipe is used as the delimiter, adding it to the end created an additional blank entry
            let newValue = {
                OPTIONS_VALUES: attribute.OPTIONS_VALUES == null ? "" : `${attribute.OPTIONS_VALUES}||`,
                OPTIONS_LABELS: attribute.OPTIONS_LABELS == null ? "" : `${attribute.OPTIONS_LABELS}||`
            }

            setAttribute((prevState) => (prevState ? {...prevState, ...newValue} : null))

        } else {

            showToast("warning", `There can not be more than ${maximumOptions} options`, "max-options")
        }
    }

    /**
     * A function that deletes an option from the list. Note that the payload comes from the
     * ConfirmButton component so the type is the general type of the onClick prop
     * @param payload - The object of information from the Button component. This contains the position of the
     * option to delete.
     */
    const handleDeleteOption = (payload: ConfirmButtonPayload): void => {

        const {id} = payload

        if (typeof id === "number") {

            // Convert the string value stored in the dataset to an array of options.
            let newOptionsValues = attribute.OPTIONS_VALUES == null ? [] : attribute.OPTIONS_VALUES.split("||")
            let newOptionsLabels = attribute.OPTIONS_LABELS == null ? [] : attribute.OPTIONS_LABELS.split("||")

            // Remove the option
            newOptionsValues.splice(id, 1)
            newOptionsLabels.splice(id, 1)

            // If we delete an option the default value may become invalid in which case we remove it
            // We have to deal with the multi and single option use case
            let newDefaultValue: string | null

            if (attribute.IS_MULTI) {
                // If multiple options are allowed then we need to check each of the revised options against each of the
                // default values, to do this we need to split the default value string into an array. Note that we do not need
                // To convert the string in the array elements to their type e.g. to an integer as the comparison is a
                // always a string to a string
                newDefaultValue = attribute.DEFAULT_VALUE != null ? attribute.DEFAULT_VALUE.split("||").filter(value => newOptionsValues.includes(value)).join("||") : null

            } else {

                // If multiple options are not allowed then we need to check each of the revised options against the efault value. Note that we do not need
                // To convert the string in the array elements to their type e.g. to an integer as the comparison is a
                // always a string to a string
                newDefaultValue = attribute.DEFAULT_VALUE != null && newOptionsValues.includes(attribute.DEFAULT_VALUE) ? attribute.DEFAULT_VALUE : null
            }

            setAttribute((prevState) => (prevState ? {
                ...prevState, ...{
                    OPTIONS_VALUES: newOptionsValues.length === 0 ? null : newOptionsValues.join("||"),
                    OPTIONS_LABELS: newOptionsLabels.length === 0 ? null : newOptionsLabels.join("||"),
                    DEFAULT_VALUE: newDefaultValue
                }
            } : null))
        }
    }

    /**
     * A function that runs when the user changes the toggle to select whether to use the schema of a dataset as the set of options.
     */
    const handleUseSchemaChange = useCallback((payload: SelectTogglePayload) => {

        const {value} = payload

        setAttribute((prevState) => {

            if (prevState == null) return null

            // If we choose to use the schema we add an option that is valid but is used in the UI to say use the schema instead
            // We also remove the default value. If the user changes to not use the schema we blank everything.
            let newValue = {OPTIONS_VALUES: value === true ? "USE_SCHEMA" : "", OPTIONS_LABELS: value === true ? "USE_SCHEMA" : "", DEFAULT_VALUE: null}

            return {...prevState, ...newValue}
        })

        // Set the validation of the things we changed to valid
        setValidation((prevState) => ({...prevState, ...{DEFAULT_VALUE: true, OPTIONS_VALUES: value === null ? false : value, OPTIONS_LABELS: value === null ? false : value}}))

    }, [setAttribute, setValidation])

    /**
     * A function that updates either an option's value or label for non-date attributes.
     *
     * @param payload - The payload from the edited option value or label. This contains the position of the
     * option and what was changed.
     */
    const handleGeneralOptionChange = useCallback((payload: SelectValuePayload): void => {

        const {name, id, isValid, value} = payload

        if (typeof id !== "number" || !(name ===  "value" || name ==="label")) return

        // Set which part of the option was edited
        const key = name === "value" ? "OPTIONS_VALUES" : "OPTIONS_LABELS"

        // Join the array back up and store the new string version
        setAttribute((prevState) => {

            if (prevState == null) return null

            // Convert the string stored version of the values or labels into an array
            // Typescript appears to have a bug in asserting types here so there is some redundant logic
            const newOptions = prevState[key] == null ? [] : (prevState[key] || "").split("||")

            // Update the element with the new value. If the option type is a number than value will be a number, so we
            // need to convert to a string to be able to concatenate the value into the list of options.
            newOptions[id] = !value ? "" : value.toString()

            // If we edit an option the default value may become invalid in which case we remove it, we only have to do this
            // when the values are being edited
            const newDefaultValue = key === "OPTIONS_LABELS" || (prevState.DEFAULT_VALUE != null && newOptions.includes(prevState.DEFAULT_VALUE)) ? prevState.DEFAULT_VALUE : null

            let newValue = {[key]: newOptions.join("||"), DEFAULT_VALUE: newDefaultValue}

            return {...prevState, ...newValue}
        })

        setValidation((prevState) => ({...prevState, ...{[key]: isValid}}))

    }, [setAttribute, setValidation])

    /**
     * A function that updates both an option's value and label for date attributes.
     * @param payload - The object of information from the SelectDate component.
     */
    const handleDateOptionChange = useCallback((payload: SelectDatePayload): void => {

        const {formatted, id, isValid, value} = payload

        if (typeof id !== "number") return

        // Join the array back up and store the new string version
        setAttribute((prevState) => {

            if (prevState == null) return null

            // Convert the string stored version of the values or labels into an array
            const newValueOptions = prevState.OPTIONS_VALUES == null ? [] : prevState.OPTIONS_VALUES.split("||")
            const newLabelOptions = prevState.OPTIONS_LABELS == null ? [] : prevState.OPTIONS_LABELS.split("||")

            // Update the elements with the new values, the values are ISO strings and the labels are formatted versions
            newValueOptions[id] = !value ? "" : value
            newLabelOptions[id] = !formatted ? "" : formatted

            // If we edit an option the default value may become invalid in which case we remove it, we only have to do this
            // when the values are being edited
            const newDefaultValue = prevState.DEFAULT_VALUE != null && newValueOptions.includes(prevState.DEFAULT_VALUE) ? prevState.DEFAULT_VALUE : null

            let newValue = {
                "OPTIONS_VALUES": newValueOptions.join("||"),
                "OPTIONS_LABELS": newLabelOptions.join("||"),
                DEFAULT_VALUE: newDefaultValue
            }

            return {...prevState, ...newValue}
        })

        setValidation((prevState) => ({...prevState, ...{OPTIONS_VALUES: isValid}}))

    }, [setAttribute, setValidation])

    /**
     * A function that is passed to a SelectValue component to use to determine if the user set option values or labels are
     * valid, this replaces the default validity checker. Option labels must be unique and an option can not be null
     * for both the label and the value.
     */
    const checkNonDateOptionValidity = useCallback((payload: SelectValueCheckValidityArgs): CheckValidityReturn => {

        // Extract the values we need from the payload, we need to check validity on what the value will be with the new
        // value in place.
        const {id, name, value} = payload

        if (typeof id !== "number") throw new TypeError("id was not set as a number")

        // To work out if the variable is valid we need to see what the labels and values will look like with the change
        // made, however the attribute that the SelectValue component has in this function are the values BEFORE the change is
        // made, so we need to check against the attribute as it will be, which means spoofing the change.

        // Convert the string stored version of the values or labels into an array
        // Typescript appears to have a bug in asserting types here so there is some redundant logic
        let newValueOptions: string | string[] = attribute.OPTIONS_VALUES == null ? [] : (attribute.OPTIONS_VALUES || "").split("||")
        let newLabelOptions: string | string[] = attribute.OPTIONS_LABELS == null ? [] : (attribute.OPTIONS_LABELS || "").split("||")

        // Update the element with the new value. If the option type is a number than value will be a number, so we
        // need to convert to a string to be able to concatenate the value into the list of options.
        if (name === "value") {
            newValueOptions[id] = !value ? "" : value.toString()
        } else {
            newLabelOptions[id] = !value ? "" : value.toString()
        }

        if (newValueOptions.findIndex((valueOption, i) => !valueOption && !newLabelOptions[i]) > -1) {
            return {isValid: false, message: `You can not have options with empty values and labels`}
        } else if (name === "label" && newLabelOptions.findIndex(labelOption => !labelOption) > -1) {
            return {isValid: false, message: `You can not have options with empty labels`}
        } else if (newLabelOptions.length !== [...new Set(newLabelOptions)].length) {
            return {isValid: false, message: `You can not have options with duplicated labels`}
        } else {
            return {isValid: true, message: ""}
        }

    }, [attribute.OPTIONS_LABELS, attribute.OPTIONS_VALUES])

    /**
     * A function that is passed to a SelectDate component to use to determine if the user set options are
     * valid, this replaces the default validity checker. Date options can not be duplicated or be null.
     */
    const checkDateOptionsValidity = useCallback((payload: SelectValueCheckValidityArgs): CheckValidityReturn => {

        // Extract the values we need from the payload, we need to check validity on what the value will be with the new
        // value in place.
        const {id, value} = payload

        if (typeof id !== "number") throw new TypeError("id was not set as a number")

        // Convert the string stored version of the values into an array
        const newValueOptions = attribute.OPTIONS_VALUES == null ? [] : attribute.OPTIONS_VALUES.split("||")
        newValueOptions[id] = !value ? "" : value.toString()

        if (newValueOptions.length !== [...new Set(newValueOptions)].length) {
            return {isValid: false, message: `You can not have options with duplicated dates`}
        } else if (newValueOptions.findIndex(valueOption => !valueOption) > -1) {
            return {isValid: false, message: `You can not have options with empty dates`}
        } else {
            return {isValid: true, message: ""}
        }

    }, [attribute.OPTIONS_VALUES])

    // Whether the user has opted to use the schema as the set of options for an attribute for datasets e.g. hidden_columns
    const canUseSchema = Boolean(attribute.OBJECT_TYPES === "DATA" && attribute.BASIC_TYPE === trac.STRING)
    const isUseSchema = Boolean(attribute.USE_OPTIONS && attribute.OPTIONS_VALUES === "USE_SCHEMA" && attribute.OPTIONS_LABELS === "USE_SCHEMA")

    return (

        // The border is just a visual aide to the user to group the menu together
        <div className="mt-1 px-4 pb-4 border">

            <HeaderTitle type={"h4"} text={"Options"}
                         tooltip={"Use the section below to create the options that will be available to the user"}>

                <div className={"d-flex"}>
                    {attribute.ID !== "business_segments" && !(canUseSchema && isUseSchema) &&
                        <Button ariaLabel={"Add option"}
                                className={"me-3"}
                                isDispatched={false}
                                onClick={handleAddOption}
                                variant={"outline-info"}
                        >
                            <Icon icon={"bi-plus-lg"}
                                  ariaLabel={false}
                                  className={"me-2"}
                            />
                            Add option
                        </Button>
                    }
                    {canUseSchema &&
                        <SelectToggle id={"USE_SCHEMA"}
                                      isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                      isDispatched={false}
                                      labelPosition={"left"}
                                      labelText={"Use schema:"}
                                      mustValidate={false}
                                      onChange={handleUseSchemaChange}
                                      showValidationMessage={false}
                                      value={isUseSchema}
                        />
                    }
                </div>

            </HeaderTitle>

            {attribute.ID === "business_segments" &&
                <Alert variant={"success"}>
                    Business segments are not set here, instead you can add new choices by editing the Business segment options dataset.
                </Alert>
            }

            {canUseSchema && isUseSchema &&
                <Alert variant={"success"}>
                    This attribute has been set to use the schema of the dataset as the set of options, additional options can not be added.
                </Alert>
            }

            {attribute.ID !== "business_segments" && !(canUseSchema && isUseSchema) &&
                <Row className={"pt-3"}>
                    {attribute.OPTIONS_VALUES == null ? [] : attribute.OPTIONS_VALUES.split("||").map((value, i) => {

                        // We only add validation messages to the last option, this covers all the options. This
                        // makes the layout a bit more compact
                        const isLast = Boolean(i === (attribute.OPTIONS_VALUES == null ? [] : attribute.OPTIONS_VALUES.split("||")).length - 1)
                        const numberOfOptions = attribute.OPTIONS_VALUES == null ? 0 : attribute.OPTIONS_VALUES.split("||").length

                        return (
                            <React.Fragment key={i}>

                                <Col xs={xsGrid} md={mdGrid} lg={lgGrid} className={"mb-1"}>
                                    {/*We have a different components based on the attribute type due to typescript*/}
                                    {/*It also means that the values will be revalidated as changing the basic type will*/}
                                    {/*trigger a new component to be mounted and validation to be checked*/}
                                    {isTracNumber(attribute.BASIC_TYPE) &&
                                        <SelectValue basicType={attribute.BASIC_TYPE}
                                                     checkValidity={checkNonDateOptionValidity}
                                                     id={i}
                                                     isDispatched={false}
                                            // This is a pure hack, when someone changes the label then the option
                                            // overall can be valid because blank values are allowed. So the key here is to
                                            // force the value part of the option to revalidate. We limit when this happens
                                            // to when the label changes from null to not null rather than every keystroke.
                                                     key={attribute.OPTIONS_LABELS == null || attribute.OPTIONS_LABELS.split("||")[i] === "" ? "null" : "not-null"}
                                                     labelText={i === 0 ? "Value" : undefined}
                                                     mustValidate={isLast}
                                                     name={"value"}
                                                     onChange={handleGeneralOptionChange}
                                                     showValidationMessage={isLast}
                                                     validationChecked={validationChecked}
                                                     value={value == null || value == "" ? null : parseFloat(value)}
                                        />
                                    }

                                    {isTracString(attribute.BASIC_TYPE) &&
                                        <SelectValue basicType={attribute.BASIC_TYPE}
                                                     checkValidity={checkNonDateOptionValidity}
                                                     id={i}
                                                     isDispatched={false}
                                            // This is a pure hack, when someone changes the label then the option
                                            // overall can be valid because blank values are allowed. So the key here is to
                                            // force the value part of the option to revalidate. We limit when this happens
                                            // to when the label changes from null to not null rather than every keystroke.
                                                     key={attribute.OPTIONS_LABELS == null || attribute.OPTIONS_LABELS.split("||")[i] === "" ? "null" : "not-null"}
                                                     labelText={i === 0 ? "Value" : undefined}
                                                     mustValidate={isLast}
                                                     name={"value"}
                                                     onChange={handleGeneralOptionChange}
                                                     showValidationMessage={isLast}
                                                     validationChecked={validationChecked}
                                                     value={value}
                                        />
                                    }

                                    {isTracDateOrDatetime(attribute.BASIC_TYPE) &&
                                        // No onChange function is needed here as the value is
                                        // set from the label when that is set.
                                        <SelectValue basicType={trac.STRING}
                                                     checkValidity={checkDateOptionsValidity}
                                                     id={i}
                                                     readOnly={true}
                                                     isDispatched={false}
                                                     labelText={i === 0 ? "Value" : undefined}
                                                     mustValidate={isLast}
                                                     name={"value"}
                                                     onChange={dummyFunction}
                                                     showValidationMessage={isLast}
                                                     validationChecked={validationChecked}
                                                     value={value}
                                                     placeHolderText={"Please select label"}
                                        />
                                    }
                                </Col>

                                {/*The empty col is for layout optimisation*/}
                                <Col xs={6} md={5} lg={4} className={"mb-1"}>
                                    {/*We have a different components based on the attribute type due to typescript*/}
                                    {/*It also means that the values will be revalidated as changing the basic type will*/}
                                    {/*trigger a new component to be mounted and validation to be checked*/}
                                    {isTracNumber(attribute.BASIC_TYPE) &&
                                        <SelectValue basicType={trac.STRING}
                                                     checkValidity={checkNonDateOptionValidity}
                                                     id={i}
                                                     isDispatched={false}
                                                     labelText={i === 0 ? "Label" : undefined}
                                                     mustValidate={isLast}
                                                     name={"label"}
                                                     onChange={handleGeneralOptionChange}
                                                     showValidationMessage={isLast}
                                                     validationChecked={validationChecked}
                                                     value={attribute.OPTIONS_LABELS == null ? null : attribute.OPTIONS_LABELS.split("||")[i]}
                                        />
                                    }

                                    {isTracString(attribute.BASIC_TYPE) &&
                                        <SelectValue basicType={trac.STRING}
                                                     checkValidity={checkNonDateOptionValidity}
                                                     id={i}
                                                     isDispatched={false}
                                                     labelText={i === 0 ? "Label" : undefined}
                                                     mustValidate={isLast}
                                                     name={"label"}
                                                     onChange={handleGeneralOptionChange}
                                                     showValidationMessage={isLast}
                                                     validationChecked={validationChecked}
                                                     value={attribute.OPTIONS_LABELS == null ? null : attribute.OPTIONS_LABELS.split("||")[i]}
                                        />
                                    }

                                    {isTracDateOrDatetime(attribute.BASIC_TYPE) &&
                                        <SelectDate basicType={attribute.BASIC_TYPE}
                                                    formatCode={attribute.FORMAT_CODE == null || !isDateFormat(attribute.FORMAT_CODE) ? null : attribute.FORMAT_CODE}
                                                    id={i}
                                                    isDispatched={false}
                                                    labelText={i === 0 ? "Label" : undefined}
                                                    name={"label"}
                                                    onChange={handleDateOptionChange}
                                                    showValidationMessage={isLast}
                                                    validationChecked={validationChecked}
                                                    value={attribute.OPTIONS_VALUES == null ? null : attribute.OPTIONS_VALUES.split("||")[i]}
                                        />
                                    }
                                </Col>

                                <Col xs={1} lg={2}
                                     className={`align-self-center ${isLast && i > 0 ? "mb-4 pb-1" : !isLast && i === 0 ? "mt-4 pt-1" : ""}`}>
                                    {/*You can not delete the last option*/}
                                    {numberOfOptions > 1 &&
                                        <ConfirmButton ariaLabel={"Delete option"}
                                                       className={"m-0 p-0"}
                                                       description={"Are you sure that you want to delete this option?"}
                                                       id={i}
                                                       onClick={handleDeleteOption}
                                                       variant={"link"}
                                        >
                                            <Icon icon={"bi-trash3"} ariaLabel={false}/>
                                        </ConfirmButton>
                                    }
                                </Col>

                            </React.Fragment>
                        )
                    })}
                </Row>
            }

        </div>
    )
};

ConfigureAttributeOptions.propTypes = {

    attribute: PropTypes.object.isRequired,
    setAttribute: PropTypes.func.isRequired,
    setValidation: PropTypes.func.isRequired,
    validationChecked: PropTypes.bool.isRequired
};

export default ConfigureAttributeOptions;