/**
 * A component that shows a modal that allows the user to select a version of a model from a list of options.
 * @module ModelOverlayModal
 * @category RunAFlowScene component
 */

import {Button} from "../../../components/Button";
import Modal from "react-bootstrap/Modal";
import {ObjectDetails} from "../../../components/ObjectDetails";
import PropTypes from "prop-types";
import React from "react";
import {SelectOption} from "../../../components/SelectOption";
import {addModelOrInputOption, setModel} from "../store/runAFlowStore";
import {TextBlock} from "../../../components/TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../../types/types_hooks";

/**
 * An interface for the props of the ModelVersionModal component.
 */
export interface Props {

    /**
     * The key of the model in the flow to show the options for.
     */
    modelKey: string
    /**
     * Whether to show the modal.
     */
    show: boolean
    /**
     * The function to close the modal.
     */
    toggle: React.Dispatch<{ type: "showVersionModal" }>
}

export const ModelVersionModal = (props: Props) => {

    const {show, toggle, modelKey} = props

    // Get what we need from the store - ps destructuring is awesome!
    const {
        modelOptions: {[modelKey]: options},
        selectedModelOptions: {[modelKey]: selectedOption},
        status
    } = useAppSelector(state => state["runAFlowStore"].models)

    return (

        <Modal size={"lg"} show={show} onHide={() => toggle({type: "showVersionModal"})}>

            <Modal.Header closeButton>
                <Modal.Title>
                    Select a version of the model to use
                </Modal.Title>
            </Modal.Header>

            <Modal.Body className={"mx-xs-3 mx-lg-5 pb-5"}>

                <TextBlock>
                    You can use the menu below to change which version of this model to use in the flow.
                </TextBlock>

                <SelectOption basicType={trac.STRING}
                              className={"pb-3"}
                              hideDisabledOptions={true}
                              id={modelKey}
                              isCreatable={true}
                              isDispatched={true}
                              isLoading={status === "pending"}
                              mustValidate={true}
                              objectType={trac.ObjectType.MODEL}
                              onChange={setModel}
                              onCreateNewOption={addModelOrInputOption}
                              options={options}
                              showValidationMessage={false}
                              value={selectedOption}
                              validateOnMount={false}
                />

                {selectedOption &&
                    <ObjectDetails bordered={false}
                                   metadata={selectedOption.tag}
                                   striped={false}
                                   title={"Model summary"}
                    />
                }

            </Modal.Body>
            <Modal.Footer>

                <Button ariaLabel={"Close"}
                        isDispatched={false}
                        onClick={() => toggle({type: "showVersionModal"})}
                        variant={"secondary"}
                >
                    Close
                </Button>

            </Modal.Footer>
        </Modal>
    )
};

ModelVersionModal.propTypes = {

    modelKey: PropTypes.string.isRequired,
    show: PropTypes.bool.isRequired,
    toggle: PropTypes.func.isRequired
};