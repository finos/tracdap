/**
 * A component that shows a modal with the attribute selected to view shown, this is so the user can
 * check that the look and behaviour of the attribute is correct. This shows what the attribute will
 * look like after editing, not the version that is saved in TRAC, this is important so the user can
 * see their edits before saving.
 *
 * @module
 * @category Component
 */

import {Button} from "../../../components/Button";
import Modal from "react-bootstrap/Modal";
import type {DeepWritable, Option, SelectPayload} from "../../../../types/types_general";
import {ParameterOption} from "../../../components/ParameterMenu/ParameterOption";
import PropTypes from "prop-types";
import React, {useEffect, useMemo, useState} from "react";
import Row from "react-bootstrap/Row";
import {useAppSelector} from "../../../../types/types_hooks";
import {processAttributes} from "../../../utils/utils_attributes_and_parameters";

/**
 * An interface for the props of the ViewAttributeModal component.
 */
interface Props {

    /**
     * The ID of the attribute to view.
     */
    attributeId?: string
    /**
     * Whether to show the modal or not.
     */
    show: boolean
    /**
     * A function that toggles whether the modal is shown or not.
     */
    toggle: React.Dispatch<React.SetStateAction<{ show: boolean, key?: string }>>
}

export const ViewAttributeModal = (props: Props) => {

    const {show, attributeId, toggle} = props

    // Get what we need from the store, note we are getting the edited version
    const {data} = useAppSelector(state => state["applicationSetupStore"].editor.items.ui_attributes_list)
    const {ui_business_segment_options} = useAppSelector(state => state["applicationSetupStore"].editor.items)

    // The needs to be memoized otherwise each change in value triggers a render that triggers the useEffect that resets the
    // value to the default
    const parameterVersion = useMemo(() => {

        // Get the attribute to view, notice that we are getting the edited value not the saved value.
        const editedAttribute = attributeId ? data.find(row => row.ID === attributeId) : undefined

        // Convert the edited attribute into a parameter we can pass to the ParameterMenu component
        return editedAttribute && attributeId ? processAttributes([editedAttribute], ui_business_segment_options.data, ui_business_segment_options.fields)[attributeId] : undefined

    }, [attributeId, data, ui_business_segment_options.data, ui_business_segment_options.fields])

    // The state of the value of the attribute
    const [value, setValue] = useState<DeepWritable<SelectPayload<Option, boolean>>["value"]>(null)

    /**
     * A function that updates the user value to the local state. This
     * @param payload - The payload from the Select component when the user changes the value.
     */
    const handleOnChange = (payload: DeepWritable<SelectPayload<Option, boolean>>): void => {
        setValue(payload.value)
    }

    useEffect(() => {

        setValue(parameterVersion?.defaultValue || null)

    }, [parameterVersion])

    return (

        <Modal size={"lg"} show={show} onHide={() => toggle((prevState) => ({...prevState, show: false}))}>

            <Modal.Header closeButton>
                <Modal.Title>
                    Viewing {parameterVersion === undefined ? "an attribute" : `attribute '${parameterVersion.name}'`}
                </Modal.Title>
            </Modal.Header>

            <Modal.Body className={"mx-5"}>

                <Row>

                    {parameterVersion &&
                        <ParameterOption baseWidth={6}
                                         basicType={parameterVersion.basicType}
                                         className={"mt-3"}
                                         description={parameterVersion.description}
                                         disabled={parameterVersion.disabled}
                                         isDispatched={false}
                                         formatCode={parameterVersion.formatCode}
                                         hidden={false}
                                         isMulti={parameterVersion.isMulti}
                                         maximumValue={parameterVersion.maximumValue}
                                         minimumValue={parameterVersion.minimumValue}
                                         mustValidate={parameterVersion.mustValidate}
                                         name={parameterVersion.name}
                                         onChange={handleOnChange}
                                         options={parameterVersion.options}
                                         parameterKey={parameterVersion.id}
                                         specialType={parameterVersion.specialType}
                                         textareaRows={parameterVersion.textareaRows}
                                         tooltip={parameterVersion.tooltip}
                                         validationChecked={true}
                                         value={value}
                                         widthMultiplier={parameterVersion.widthMultiplier}
                        />
                    }
                </Row>

            </Modal.Body>

            <Modal.Footer>
                <Button ariaLabel={"Close attribute viewer"}
                        variant={"secondary"}
                        onClick={() => toggle((prevState) => ({...prevState, show: false}))}
                        isDispatched={false}
                >
                    Close
                </Button>
            </Modal.Footer>
        </Modal>
    )
};

ViewAttributeModal.propTypes = {

    key: PropTypes.string,
    show: PropTypes.bool.isRequired,
    toggle: PropTypes.func.isRequired
};