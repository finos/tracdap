/**
 * This component shows a modal with a business segment editor inside, this allows the user to edit a segment's
 * ID and name.
 * @module
 * @category Component
 */

import {Button} from "../../../components/Button";
import {CheckValidityReturn, SelectValueCheckValidityArgs} from "../../../../types/types_general";
import Col from "react-bootstrap/Col";
import {editBusinessSegment, saveEditedRowToDataset} from "../store/applicationSetupStore";
import {getFieldLabelFromSchema} from "../../../utils/utils_schema";
import {Icon} from "../../../components/Icon";
import Modal from "react-bootstrap/Modal";
import PropTypes from "prop-types";
import React, {useCallback, useState} from "react";
import Row from "react-bootstrap/Row";
import {SelectValue} from "../../../components/SelectValue";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";
import {isKeyOf} from "../../../utils/utils_trac_type_chckers";

/**
 * An interface for the props of the ConfigureBusinessSegmentModal component.
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

export const ConfigureBusinessSegmentModal = (props: Props) => {

    console.log("Rendering ConfigureBusinessSegmentModal")

    const {show, toggle} = props

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store

    const {fields, row, variable, validation, index} = useAppSelector(state => state["applicationSetupStore"].editor.items.ui_business_segment_options)

    // Whether to show the validation messages
    const [validationChecked, setValidationChecked] = useState<boolean>(false)

    /**
     * A function that runs when the user clicks to save their changes to the business segment definition. This checks
     * that the values set for the ID and NAME are valid and if so updates the store and closes the modal.
     */
    const handleSaveBusinessSegment = (): void => {

        if (row == null) throw new TypeError("The business segment entry being saved is null, this is not allowed")
        if (typeof index != "number") throw new TypeError("The index for accessing validation information is not a number")

        // Has the validation that was stored in the Redux store when the value was changed set to true.
        // If not prevent the user saving their changes.
        if (!validation.isValid[index][variable]) {

            setValidationChecked(true)

        } else {

            setValidationChecked(false)
            dispatch(saveEditedRowToDataset(row))
            toggle(false)
       }
    }

    /**
     * A function that is passed to a SelectValue component to use to determine if the user set segment values are
     * valid, this replaces the default validity checker. There is a weird thing here where the function is looking
     * across the two inputs, the ID and the name. So either both are OK or both are invalid. So when the validity
     * is saved in the store we need to update the validity of both. Otherwise, one value that was invalid before the
     * second value was changed will still be listed as invalid.
     */
    const checkValidity = useCallback((payload: SelectValueCheckValidityArgs): CheckValidityReturn => {

        // Extract the values we need from the payload, id is the full variable name e.g. "GROUP_01_ID" while variable
        // is the generic name e.g. "GROUP_01". value is the string to be validated
        const {id, name: variable, value} = payload

        const variable_id = `${variable}_ID`
        const variable_name = `${variable}_NAME`

        // Now do some type checking to appease the Typescript gods because this is a generic payload
        if (row == null) {
            throw new TypeError((`The row being edited was missing, this is not allowed when editing a business segment`))
        }

        if (!(typeof id !== "number" && isKeyOf(row, id) && typeof value != "number")) {
            throw new TypeError((`The value to be checked is a number or the variable to update is not in the dataset.`))
        }

        if (!isKeyOf(row, variable_id) || !isKeyOf(row, variable_name)) {
            throw new TypeError((`The variable to update is not in the dataset.`))
        }

        // Now here's the thing that took me 3 hours to dig into and fix. To work out if the value is valid we need to see what
        // the row will look like with the change made, however the row that the SelectValue component has in this function is the
        // row BEFORE the change is made, so we need to check against the row as it will be, which means spoofing the change.
        // So first we copy the row.
        let newRow = {...row}
        // Make the change
        newRow[id] = value

        const value_id = newRow[variable_id]
        const value_name = newRow[variable_name]

        // Apply the test to see if the ID and name are valid
        if ((typeof value_id === "string" && value_id.length > 0 && value_name == null) || (typeof value_name === "string" && value_name.length > 0 && value_id == null)) {

            return {isValid: false, message: "The ID and name must either be both filled in or both blank"}

        } else {

            return {isValid: true, message: ""}
        }

    }, [row])

    return (

        <Modal show={show} onHide={() => toggle(false)}>

            <Modal.Header closeButton>
                <Modal.Title>
                    Edit business segment definition
                </Modal.Title>
            </Modal.Header>

            <Modal.Body className={"mx-5"}>

                <Row>
                    <Col xs={12} className={"my-4"}>
                        <SelectValue basicType={trac.STRING}
                                     checkValidity={checkValidity}
                                     id={`${variable}_ID`}
                                     labelText={getFieldLabelFromSchema(fields, `${variable}_NAME`, `${variable} segment`) + " ID"}
                                     mustValidate={false}
                                     name={`${variable}`}
                                     onChange={editBusinessSegment}
                                     showValidationMessage={false}
                                     validateOnMount={false}
                                     validationChecked={validationChecked}
                                     value={row ? row[`${variable}_ID`] : null}

                        />
                    </Col>
                    <Col xs={12} className={"mb-4"}>
                        <SelectValue basicType={trac.STRING}
                                     checkValidity={checkValidity}
                                     id={`${variable}_NAME`}
                                     labelText={getFieldLabelFromSchema(fields, `${variable}_NAME`, `${variable} segment`) + " name"}
                                     mustValidate={true}
                                     name={`${variable}`}
                                     onChange={editBusinessSegment}
                                     showValidationMessage={validationChecked}
                                     validateOnMount={true}
                                     validationChecked={validationChecked}
                                     value={row ? row[`${variable}_NAME`] : null}
                        />
                    </Col>
                </Row>

            </Modal.Body>

            <Modal.Footer>
                <Button ariaLabel={"Close business segment editor"}
                        variant={"secondary"}
                        onClick={() => toggle(false)}
                        isDispatched={false}
                >
                    Close
                </Button>
                <Button ariaLabel={"Save business segment editor"}
                        variant={"info"}
                        onClick={handleSaveBusinessSegment}
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

ConfigureBusinessSegmentModal.propTypes = {

    show: PropTypes.bool.isRequired,
    toggle: PropTypes.func.isRequired,
};