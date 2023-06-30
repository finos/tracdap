/**
 * A component that shows a modal with an attribute editor inside, this allows the user to edit an attribute
 * so that its options and object types can be set.
 * @module
 * @category Component
 */

import {Alert} from "../../../components/Alert";
import {Button} from "../../../components/Button";
import Col from "react-bootstrap/Col";
import {ConfigureAttributeDefaultValue} from "./ConfigureAttributeDefaultValue";
import {ConfigureAttributeGeneral} from "./ConfigureAttributeGeneral";
import ConfigureAttributeOptions from "./ConfigureAttributeOptions";
import ConfigureAttributeValidation from "./ConfigureAttributeValidation";
import ConfigureAttributeVisibility from "./ConfigureAttributeVisibility";
import ConfigureAttributeType from "./ConfigureAttributeType";
import {Icon} from "../../../components/Icon";
import {isTracDateOrDatetime, isTracNumber, isTracString,} from "../../../utils/utils_trac_type_chckers";
import Modal from "react-bootstrap/Modal";
import {NumberFormatEditor} from "../../../components/NumberFormatEditor";
import {objectsContainsValue} from "../../../utils/utils_object";
import {showToast} from "../../../utils/utils_general";
import PropTypes from "prop-types";
import React, {useEffect, useMemo, useState} from "react";
import Row from "react-bootstrap/Row";
import {saveEditedRowToDataset} from "../store/applicationSetupStore";
import {UiAttributesListRow} from "../../../../types/types_attributes_and_parameters";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";
import {SelectValuePayload} from "../../../../types/types_general";

/**
 * An interface for the props of the ConfigureAttributeModal component.
 */
interface Props {

    /**
     * Whether to show the modal or not.
     */
    show: boolean
    /**
     * A function that toggles whether the modal is shown or not.
     */
    toggle: React.Dispatch<React.SetStateAction<boolean>>
}

export const ConfigureAttributeModal = (props: Props) => {

    const {show, toggle} = props

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {row, fields} = useAppSelector(state => state["applicationSetupStore"].editor.items.ui_attributes_list)

    // Local state of the attribute being edited
    // The row in the dataset that we are going to edit
    const [attribute, setAttribute] = useState<null | UiAttributesListRow>(row)
    // The validation status of each property of the attribute, nothing can be saved if this contains a false
    const [validation, setValidation] = useState<Partial<Record<keyof UiAttributesListRow, boolean>>>({})
    // Whether to show the validation messages
    const [validationChecked, setValidationChecked] = useState(false)

    /**
     * A hook that runs when the component is mounted or when the row being edited changes, this syncs
     * the row being edited with the local state version.
     */
    useEffect((): void => {

        setValidation({})
        setValidationChecked(false)
        if (row != null) setAttribute(row)

    }, [row])

    /**
     * An object that contains labels for the fields in the ui_attributes_list fields dataset in a dictionary. Using
     * useMemo means that the values only get updated if the schema changes. This is quicker than finding the
     * right label in the render stage below.
     */
    const labels = useMemo((): Record<string, string> => {

        let labels: Record<string, string> = {}

        fields.forEach(field => {
            if (field.label != null && field.fieldName != null) labels[field.fieldName] = field.label
        })

        return labels

    }, [fields])

    /**
     * A function that handles changes to the number format as managed by the NumberFormatEditor component. This
     * sends a string representation of the format to save in the attribute definition.
     * @param payload - The object of information from the NumberFormatEditor component.
     */
    const handleNumberFormatCodeChange = (payload: SelectValuePayload<string>): void => {

        const {id, value} = payload

        if (id !== "FORMAT_CODE") return

        // Update the value
        setAttribute((prevState) => (prevState ? {...prevState, [id]: value} : null))
    }

    /**
     * A function that runs when the user clicks to save their changes to the attribute definition. This checks
     * that the values set for the properties are valid and if so updates the store and closes the modal.
     */
    const handleSaveAttribute = (): void => {

        if (attribute != null) {

            let newAttribute = {...attribute}

            console.log(validation)
            if (objectsContainsValue(validation, false)) {

                if (!validationChecked) setValidationChecked(true)
                showToast("error", "There are problems with some of your values, please fix the issues shown and try again.", "not-valid")

            } else {

                if (validationChecked) setValidationChecked(false)
                dispatch(saveEditedRowToDataset(newAttribute))
                toggle(false)
            }
        }
    }

    return (

        <Modal size={"xl"} show={show} onHide={() => toggle(false)} backdrop="static">

            <Modal.Header closeButton>
                <Modal.Title>
                    Edit an attribute&apos;s definition
                </Modal.Title>
            </Modal.Header>

            <Modal.Body className={"mx-5"}>

                {/*This is needed due to a side effect of how the component is built. When the component loads */}
                {/*all the Select components mount and then send back their validation information to the state of this*/}
                {/*component. However, without this condition in, the useEffect function has not copied */}
                {/*the row to be edited into state so the validation messages are all wrong and not corrected */}
                {/*until the Select components have their values changed. So what we do with this condition is only */}
                {/*mount the Select components after the row to be edited has been copied into state. This way the
                {/*the validation info is right from the start.*/}
                {attribute != null && Object.keys(attribute).length > 0 &&

                    <React.Fragment>

                        {attribute.RESERVED_FOR_APPLICATION &&
                            <Alert className={"mt-3"} variant={"success"}>
                                This attribute is reserved by the user interface, there are restrictions in place that limit how it can be edited.
                            </Alert>
                        }

                        <ConfigureAttributeGeneral attribute={attribute}
                                                   labels={labels}
                                                   setAttribute={setAttribute}
                                                   setValidation={setValidation}
                                                   validationChecked={validationChecked}
                        />

                        <ConfigureAttributeType attribute={attribute}
                                                labels={labels}
                                                setAttribute={setAttribute}
                                                setValidation={setValidation}
                                                validationChecked={validationChecked}
                        />

                        {/*If the user has selected to set the attribute from a set of options, allow the user to define */}
                        {/*their own set. Booleans are excluded as their set is already defined.*/}
                        {(isTracNumber(attribute.BASIC_TYPE) || isTracString(attribute.BASIC_TYPE) || isTracDateOrDatetime(attribute.BASIC_TYPE)) && attribute.USE_OPTIONS &&
                            <Row><Col xs={12}>
                                <ConfigureAttributeOptions attribute={attribute}
                                                           setAttribute={setAttribute}
                                                           setValidation={setValidation}
                                                           validationChecked={validationChecked}
                                />
                            </Col>
                            </Row>
                        }

                        {/*You can only specify a number format when options are not being used, the format for options is whatever the user types into the label*/}
                        {isTracNumber(attribute.BASIC_TYPE) && !attribute.USE_OPTIONS &&
                            <Row>
                                <Col xs={12} className={"mb-4 pb-1"}>
                                    <NumberFormatEditor id={"FORMAT_CODE"}
                                                        formatCode={attribute.FORMAT_CODE}
                                                        onChange={handleNumberFormatCodeChange}
                                                        returnAs={"string"}
                                    />
                                </Col>
                            </Row>
                        }

                        <ConfigureAttributeDefaultValue attribute={attribute}
                                                        setAttribute={setAttribute}
                                                        setValidation={setValidation}
                                                        validationChecked={validationChecked}
                        />

                        {/*If we are using options we don't validate them, we assume that users set valid options*/}
                        {!attribute.USE_OPTIONS && !attribute.IS_MULTI &&
                            <ConfigureAttributeValidation attribute={attribute}
                                                          labels={labels}
                                                          setAttribute={setAttribute}
                                                          setValidation={setValidation}
                                                          validationChecked={validationChecked}
                            />
                        }

                        <ConfigureAttributeVisibility attribute={attribute}
                                                      labels={labels}
                                                      setAttribute={setAttribute}
                                                      setValidation={setValidation}
                                                      validationChecked={validationChecked}
                        />

                    </React.Fragment>
                }

            </Modal.Body>

            <Modal.Footer>
                <Button ariaLabel={"Close attribute editor"}
                        variant={"secondary"}
                        onClick={() => toggle(false)}
                        isDispatched={false}
                >
                    Close
                </Button>
                <Button ariaLabel={"Save attribute editor"}
                        variant={"info"}
                        onClick={handleSaveAttribute}
                        isDispatched={false}
                >
                    <Icon icon={"bi-save"}
                          ariaLabel={false}
                          className={"me-2"}
                    />
                    Save
                </Button>
            </Modal.Footer>
        </Modal>
    )
};

ConfigureAttributeModal.propTypes = {

    show: PropTypes.bool.isRequired,
    toggle: PropTypes.func.isRequired
};