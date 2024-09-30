import {AsString, Option,} from "../../../../types/types_general";
import {BooleanOptions} from "../../../../config/config_general";
import Col from "react-bootstrap/Col";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {isDateFormat, isOption, isTracNumber,} from "../../../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React, {useCallback, useMemo} from "react";
import Row from "react-bootstrap/Row";
import {SingleValue} from "react-select";
import {SelectDate} from "../../../components/SelectDate";
import {SelectOption} from "../../../components/SelectOption";
import {SelectToggle} from "../../../components/SelectToggle";
import {SelectValue} from "../../../components/SelectValue";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../../types/types_hooks";
import {UiAttributesListRow} from "../../../../types/types_attributes_and_parameters";

/**
 * This component shows the 'Visibility' section of the ConfigureAttributeModal component, this allows the user to set
 * what the rules are for when the attribute is visible.
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
    setValidation: React.Dispatch<React.SetStateAction<Partial<Record<keyof UiAttributesListRow, boolean >>>>
    /**
     * Whether to form has been validated and then show any in valid messages.
     */
    validationChecked: boolean
};

const ConfigureAttributeVisibility = (props: Props) => {

    const {attribute, validationChecked, labels, setAttribute, setValidation} = props

    // Get what we need from the store
    const {data} = useAppSelector(state => state["applicationSetupStore"].editor.items.ui_attributes_list)

    /**
     * An array that contains options for all the attributes in the dataset. This is used when setting the
     * visibility options to allow the user to say that the attribute needs to be set when another
     * attribute is set to a particular value.
     */
    const attributeOptions = useMemo((): Option<string, trac.BasicType>[] => {

        let options: Option<string, trac.BasicType>[] = []

        // We use forEach rather than map so we can filter the null values out and Typescript understands the types
        // Alternatively an assertion needs to be used.
        data.forEach(row => {

            if (row.ID != null && row.NAME != null && row.BASIC_TYPE != null) {
                options.push({
                    value: row.ID,
                    label: row.NAME ? row.NAME.toString() : "No label set",
                    type: row.BASIC_TYPE,
                })
            }
        })

        return options

    }, [data])

    // The type of the LINKED_TO_ID attribute, needed to know what type of input box should be shown to
    // set LINKED_TO_VALUE. This will be a BasicType enum.
    const linkedAttributeType = attributeOptions.find(option => option.value === attribute.LINKED_TO_ID)?.type

    /**
     * A function that handles changes to whether the user sets the attribute to be hidden from the user interface,
     * so they can not set it. There is additional logic to handle when turning this to false.
     * @param payload - The object of information from the SelectToggle component.
     */
    const handleHiddenChange = useCallback((payload: { id: "HIDDEN", isValid: boolean, value: boolean | null }): void => {

        const {id, value, isValid} = payload

        // These are the two objects that we are going to create that contain the required changes
        let newValue: Partial<UiAttributesListRow> = {HIDDEN: value || false}
        let newValidation: { [key in keyof Partial<UiAttributesListRow>]: boolean } = {[id]: isValid}

        // Note that all the update logic is inside a useState function, this allows us to access the existing 
        // attribute values. 
        setAttribute((prevState) => {

            if (prevState == null) return null

            // When turning the visibility off we remove any values for LINKED_TO_ID and LINKED_TO_VALUE
            if (value !== true) {
                newValue.LINKED_TO_ID = null
                newValue.LINKED_TO_VALUE = null
            }

            // When turning the visibility off we set the validation of LINKED_TO_VALUE & LINKED_TO_VALUE to true (OK). A 
            // hidden attribute with no rule about when to show it is still valid.
            if (value !== true) {
                newValidation.LINKED_TO_ID = true
                newValidation.LINKED_TO_VALUE = true
            }

            return {...prevState, ...newValue}
        })

        setValidation((prevState) => ({
            ...prevState,
            ...newValidation
        }))

    }, [setAttribute, setValidation])

    /**
     * A function that handles changes to the attribute that can trigger the attribute widget to be shown to the user.
     * @param payload - The object of information from the SelectOption component.
     */
    const handleLinkedIdChange = useCallback((payload: { id: "LINKED_TO_ID", isValid: boolean, value: SingleValue<Option<string, trac.BasicType>> }): void => {

        const {id, value} = payload

        // Get the value from the selected option
        let newValue = {
            [id]: value != null ? value.value : null,
            // Reset the LINKED_TO_VALUE value if LINKED_TO_ID is changed as otherwise it might not be valid
            LINKED_TO_VALUE: null
        }

        // Update the selection
        setAttribute((prevState) => (prevState != null ? {...prevState, ...newValue} : null))

    }, [setAttribute])

    /**
     * A function that handles changes to the value that triggers the attribute widget to be shown to the user.
     * @param payload - The object of information from the SelectValue component.
     */
    const handleLinkedValueChange = useCallback((payload: { id: "LINKED_TO_VALUE", isValid: boolean, value: number | string | null | Option<boolean, AsString> }): void => {

        const {value} = payload

        let newValue: { "LINKED_TO_VALUE": string | null } = {LINKED_TO_VALUE: null}

        // Options are used for booleans where the LINKED_TO_VALUE is set via an option
        if (isOption(value)) {
            newValue.LINKED_TO_VALUE = value.details.asString
        } else if (value != null) {
            newValue.LINKED_TO_VALUE = value.toString()
        }

        // Update the value
        setAttribute((prevState) => (prevState != null ? {...prevState, ...newValue} : null))

    }, [setAttribute])

    return (

        <React.Fragment>

            <HeaderTitle type={"h4"}
                         text={"Visibility"}
                         tooltip={"Attributes can be hidden from the user in which case they will be set with their default value. Attributes can also be set to be required only if another attribute is set to a specific value."}/>

            <Row className={"mb-5"}>
                <Col xs={12} sm={4} className={"align-self-sm-end"}>
                    <SelectToggle id={"HIDDEN"}
                                  isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                  isDispatched={false}
                                  labelText={labels.HIDDEN}
                                  mustValidate={false}
                                  onChange={handleHiddenChange}
                                  showValidationMessage={true}
                                  value={attribute.HIDDEN}
                                  validationChecked={validationChecked}
                    />
                </Col>

                {attribute.HIDDEN &&

                    <React.Fragment>
                        <Col xs={6} sm={4} className={"align-self-sm-end"}>
                            <SelectOption basicType={trac.STRING}
                                          id={"LINKED_TO_ID"}
                                          isClearable={true}
                                          isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                          isDispatched={false}
                                          labelText={labels.LINKED_TO_ID}
                                          onChange={handleLinkedIdChange}
                                          options={attributeOptions.filter(option => option.value !== attribute.ID)}
                                          showValidationMessage={true}
                                          value={attributeOptions.find(option => option.value === attribute.LINKED_TO_ID)}
                            />
                        </Col>

                        <Col xs={6} sm={4} className={"align-self-sm-end"}>
                            {/*We have different components to match the type of the attribute*/}
                            {isTracNumber(linkedAttributeType) &&
                                <SelectValue basicType={linkedAttributeType}
                                             id={"LINKED_TO_VALUE"}
                                             isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                             isDispatched={false}
                                             labelText={labels.LINKED_TO_VALUE}
                                             onChange={handleLinkedValueChange}
                                             showValidationMessage={true}
                                             value={attribute.LINKED_TO_VALUE ? parseFloat(attribute.LINKED_TO_VALUE) : null}
                                />
                            }

                            {linkedAttributeType === trac.STRING &&
                                <SelectValue basicType={linkedAttributeType}
                                             id={"LINKED_TO_VALUE"}
                                             isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                             isDispatched={false}
                                             labelText={labels.LINKED_TO_VALUE}
                                             onChange={handleLinkedValueChange}
                                             showValidationMessage={true}
                                             value={attribute.LINKED_TO_VALUE}
                                />
                            }

                            {linkedAttributeType === trac.DATE &&
                                <SelectDate basicType={linkedAttributeType}
                                            formatCode={attribute.FORMAT_CODE == null || !isDateFormat(attribute.FORMAT_CODE) ? null : attribute.FORMAT_CODE}
                                            id={"LINKED_TO_VALUE"}
                                            isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                            isDispatched={false}
                                            labelText={labels.LINKED_TO_VALUE}
                                            onChange={handleLinkedValueChange}
                                            showValidationMessage={true}
                                            value={attribute.LINKED_TO_VALUE}
                                />
                            }

                            {linkedAttributeType === trac.BOOLEAN &&
                                <SelectOption basicType={linkedAttributeType}
                                              id={"LINKED_TO_VALUE"}
                                              isDisabled={attribute.RESERVED_FOR_APPLICATION}
                                              isDispatched={false}
                                              labelText={labels.LINKED_TO_VALUE}
                                              onChange={handleLinkedValueChange}
                                              options={BooleanOptions}
                                              showValidationMessage={true}
                                              value={BooleanOptions.find((option) => option.details.asString === attribute.DEFAULT_VALUE)}
                                />
                            }
                        </Col>

                    </React.Fragment>
                }
            </Row>
        </React.Fragment>
    )
};

ConfigureAttributeVisibility.propTypes = {

    attribute: PropTypes.object.isRequired,
    labels: PropTypes.object.isRequired,
    setAttribute: PropTypes.func.isRequired,
    setValidation: PropTypes.func.isRequired,
    validationChecked: PropTypes.bool.isRequired
}

export default ConfigureAttributeVisibility;