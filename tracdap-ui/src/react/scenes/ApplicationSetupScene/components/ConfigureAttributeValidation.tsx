import Col from "react-bootstrap/Col";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {isDateFormat, isTracNumber, isTracString} from "../../../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React, {useCallback} from "react";
import Row from "react-bootstrap/Row";
import {SelectDate} from "../../../components/SelectDate";
import {SelectToggle} from "../../../components/SelectToggle";
import {SelectDatePayload, SelectTogglePayload, SelectValuePayload} from "../../../../types/types_general";
import {SelectValue} from "../../../components/SelectValue";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {UiAttributesListRow} from "../../../../types/types_attributes_and_parameters";

/**
 * This component shows the 'Validation' section of the ConfigureAttributeModal component, this allows the user to set
 * what the rules are that the user set value needs to pass.
 */

type Props = {

    /**
     * The attribute being edited.
     */
    attribute: UiAttributesListRow
    /**
     * Labels for each form element based on the fieldLabel entries in the dataset schema.
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
     * Whether to form has been validated and then show any in valid messages.
     */
    validationChecked: boolean
};

const ConfigureAttributeValidation = (props: Props) => {

    const {attribute, validationChecked, labels, setAttribute, setValidation} = props

    /**
     * A function that handles changes to whether the values for the attribute
     * need to be validated. There is additional logic to handle when turning this to false.
     * @param payload - The object of information from the SelectToggle component.
     */
    const handleMustValidateChange = useCallback((payload: SelectTogglePayload): void => {

        const {id, value} = payload

        if (id !== "MUST_VALIDATE") return

        const newValue: { MUST_VALIDATE: boolean, MINIMUM_VALUE?: null, MAXIMUM_VALUE?: null } = {[id]: value || false}

        // When turning the validation off we remove any values for MINIMUM_VALUE and MAXIMUM_VALUE
        if (value !== true) {
            newValue.MINIMUM_VALUE = null
            newValue.MAXIMUM_VALUE = null
        }

        setAttribute((prevState) => (prevState != null ? {...prevState, ...newValue} : null))

        const newValidation: { MINIMUM_VALUE?: boolean, MAXIMUM_VALUE?: boolean } = {}

        // When turning the validation off we set the validation of MINIMUM_VALUE & MAXIMUM_VALUE to true (OK). These 
        // only don't validate if the values are the wrong way round.
        if (value !== true) {
            newValidation.MINIMUM_VALUE = true
            newValidation.MAXIMUM_VALUE = true
        }

        // We don't update the validation status of MUST_VALIDATE itself as it doesn't need validation
        setValidation((prevState) => ({...prevState, ...newValidation}))

    }, [setAttribute, setValidation])

    /**
     * A function that handles changes to the minimum and maximum values. Number values are passed in for string and
     * number attributes whereas for dates they will be a string.
     * @param payload - The object of information from the SelectValue or SelectDate components.
     */
    const handleMinMaxChange = useCallback((payload: SelectValuePayload | SelectDatePayload): void => {

        const {id, value, isValid} = payload

        if (!(id === "MINIMUM_VALUE" || id === "MAXIMUM_VALUE")) return

        let newValue = {[id]: value == null ? null : value.toString()}

        // Update the value and the validation
        setAttribute((prevState) => (prevState != null ? {...prevState, ...newValue} : null))
        setValidation((prevState) => ({...prevState, [id]: isValid}))

    }, [setAttribute, setValidation])

    return (

        <React.Fragment>

            <HeaderTitle type={"h4"} text={"Validation"}
                         tooltip={"When validation is active the user must set a value between the minimum and maximum values (for strings the length is used) otherwise an error will be shown. Validation for boolean values means that they must be set to true"}/>

            <Row>
                <Col xs={12} sm={4} className={"align-self-end"}>
                    <SelectToggle id={"MUST_VALIDATE"}
                                  isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                  isDispatched={false}
                                  labelText={labels.MUST_VALIDATE}
                                  onChange={handleMustValidateChange}
                                  showValidationMessage={true}
                                  value={attribute.MUST_VALIDATE}
                    />
                </Col>

                {/*Strings and numbers have minimum and maximum vales defined by integers/floats*/}
                {(isTracNumber(attribute.BASIC_TYPE) || isTracString(attribute.BASIC_TYPE)) && attribute.MUST_VALIDATE &&

                    <React.Fragment>
                        <Col xs={6} sm={4} className={"align-self-sm-end"}>

                            {/*Passing the maximumValue prop means there will be validation errors if the minimum and*/}
                            {/*maximum values are the wrong way round*/}
                            <SelectValue
                                basicType={attribute.BASIC_TYPE === trac.FLOAT ? trac.FLOAT : attribute.BASIC_TYPE === trac.DECIMAL ? trac.DECIMAL : trac.INTEGER}
                                id={"MINIMUM_VALUE"}
                                isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                isDispatched={false}
                                labelText={attribute.BASIC_TYPE === trac.STRING ? "Minimum length" : "Minimum value"}
                                onChange={handleMinMaxChange}
                                maximumValue={attribute.MAXIMUM_VALUE ? parseFloat(attribute.MAXIMUM_VALUE) : undefined}
                                mustValidate={Boolean(attribute.MINIMUM_VALUE && attribute.MAXIMUM_VALUE)}
                                showValidationMessage={true}
                                validationChecked={validationChecked}
                                value={attribute.MINIMUM_VALUE != null ? parseFloat(attribute.MINIMUM_VALUE) : null}
                            />
                        </Col>

                        <Col xs={6} sm={4} className={"align-self-sm-end"}>
                            {/*Passing the minimumValue prop means there will be validation errors if the minimum and*/}
                            {/*maximum values are the wrong way round*/}
                            <SelectValue
                                basicType={attribute.BASIC_TYPE === trac.FLOAT ? trac.FLOAT : attribute.BASIC_TYPE === trac.DECIMAL ? trac.DECIMAL : trac.INTEGER}
                                id={"MAXIMUM_VALUE"}
                                isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                isDispatched={false}
                                labelText={attribute.BASIC_TYPE === trac.STRING ? "Maximum length" : "Maximum value"}
                                onChange={handleMinMaxChange}
                                minimumValue={attribute.MINIMUM_VALUE ? parseFloat(attribute.MINIMUM_VALUE) : undefined}
                                mustValidate={Boolean(attribute.MINIMUM_VALUE && attribute.MAXIMUM_VALUE)}
                                showValidationMessage={true}
                                validationChecked={validationChecked}
                                value={attribute.MAXIMUM_VALUE != null ? parseFloat(attribute.MAXIMUM_VALUE) : null}
                            />
                        </Col>
                    </React.Fragment>
                }

                {/*Dates have minimum and maximum vales defined by dates stored as ISO strings*/}
                {attribute.BASIC_TYPE === trac.DATE && attribute.MUST_VALIDATE &&
                    <React.Fragment>
                        <Col xs={6} sm={4} className={"align-self-sm-end"}>
                            {/*Passing the maximumValue prop means that the SelectDate component will disable dates for after maximumValue*/}
                            <SelectDate basicType={attribute.BASIC_TYPE}
                                        formatCode={!isDateFormat(attribute.FORMAT_CODE) ? null : attribute.FORMAT_CODE}
                                        id={"MINIMUM_VALUE"}
                                        isClearable={true}
                                        isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                        isDispatched={false}
                                        labelText={"Minimum date"}
                                        onChange={handleMinMaxChange}
                                        maximumValue={attribute.MAXIMUM_VALUE}
                                        mustValidate={Boolean(attribute.MINIMUM_VALUE && attribute.MAXIMUM_VALUE)}
                                        showValidationMessage={true}
                                        validationChecked={validationChecked}
                                        value={attribute.MINIMUM_VALUE}
                            />
                        </Col>

                        <Col xs={6} sm={4} className={"align-self-sm-end"}>
                            {/*Passing the minimumValue prop means that the SelectDate component will disable dates for before minimumValue*/}
                            <SelectDate basicType={attribute.BASIC_TYPE}
                                        formatCode={!isDateFormat(attribute.FORMAT_CODE) ? null : attribute.FORMAT_CODE}
                                        id={"MAXIMUM_VALUE"}
                                        isClearable={true}
                                        isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                        isDispatched={false}
                                        labelText={"Maximum date"}
                                        onChange={handleMinMaxChange}
                                        minimumValue={attribute.MINIMUM_VALUE}
                                        mustValidate={Boolean(attribute.MINIMUM_VALUE && attribute.MAXIMUM_VALUE)}
                                        showValidationMessage={true}
                                        validationChecked={validationChecked}
                                        value={attribute.MAXIMUM_VALUE}
                            />
                        </Col>
                    </React.Fragment>
                }
            </Row>
        </React.Fragment>
    )
};

ConfigureAttributeValidation.propTypes = {

    attribute: PropTypes.object.isRequired,
    labels: PropTypes.object.isRequired,
    setAttribute: PropTypes.func.isRequired,
    setValidation: PropTypes.func.isRequired,
    validationChecked: PropTypes.bool.isRequired
}

export default ConfigureAttributeValidation;