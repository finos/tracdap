/**
 * A component that shows a modal that allows the user to add overlays to the outputs of a model.
 * @module ModelOverlayModal
 * @category RunAFlowScene component
 */

import {Button} from "../../../components/Button";
import {convertArrayToOptions} from "../../../utils/utils_arrays";
import {hasOwnProperty} from "../../../utils/utils_trac_type_chckers";
import Modal from "react-bootstrap/Modal";
import {Option, SelectOptionPayload} from "../../../../types/types_general";
import {OverlayBuilder} from "../../../components/OverlayBuilder/OverlayBuilder";
import PropTypes from "prop-types";
import React, {useState} from "react";
import {SelectOption} from "../../../components/SelectOption";
import {TextBlock} from "../../../components/TextBlock";
import {useAppSelector} from "../../../../types/types_hooks";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the ModelOverlayModal component.
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
    toggle: React.Dispatch<{ type: "showOverlayModal" }>
}

export const ModelOverlayModal = (props: Props) => {

    const {modelKey, show, toggle} = props

    // Get what we need from the store - ps destructuring is awesome!
    const {[modelKey]: selectedOption} = useAppSelector(state => state["runAFlowStore"].models.selectedModelOptions)

    // TODO can this get the names of the outputs from somewhere
    const modelOutputOptions = convertArrayToOptions(Object.keys(selectedOption?.tag.definition?.model?.outputs || []))

    const [selectedOutput, setSelectedOutput] = useState<null | SelectOptionPayload<Option<string>, false>>(null)

    const selectedOutputDefinition = (selectedOption != null && selectedOutput?.value != null && hasOwnProperty(selectedOption?.tag?.definition?.model?.outputs, selectedOutput.value.value)) ? selectedOption?.tag?.definition?.model?.outputs[selectedOutput.value.value] : undefined

    return (

        <Modal size={"xl"} show={show} onHide={() => toggle({type: "showOverlayModal"})}>

            <Modal.Header closeButton>
                <Modal.Title>
                    Add overlays to the selected model&apos;s outputs
                </Modal.Title>
            </Modal.Header>

            <Modal.Body className={"mx-3"}>

                <TextBlock>
                    This tool can be used to add overlays to the outputs of models in the chain, the overlaid dataset will be passed through into subsequent calculations as normal.
                    After you have selected an output to overlay, you can use the menu below to build your own overlays. Please select the output from this model that you want to
                    overlay:
                </TextBlock>

                {/*TODO if dynamic schemas are introduced then this will not be possible for these datasets*/}

                <OverlayBuilder overlayKey={selectedOutput?.value?.value}
                                schema={selectedOutputDefinition?.schema?.table?.fields || []}
                                storeKey={"runAFlow"}
                >
                    <SelectOption basicType={trac.STRING}
                                  id={modelKey}
                                  isClearable={true}
                                  isDispatched={false}
                                  mustValidate={false}
                                  options={modelOutputOptions}
                                  onChange={setSelectedOutput}
                                  showValidationMessage={false}
                                  validateOnMount={false}
                                  value={selectedOutput?.value || null}
                    />
                </OverlayBuilder>

            </Modal.Body>
            <Modal.Footer>

                <Button ariaLabel={"Close"}
                        isDispatched={false}
                        onClick={() => toggle({type: "showOverlayModal"})}
                        variant={"secondary"}
                >
                    Close
                </Button>

            </Modal.Footer>
        </Modal>
    )
};

ModelOverlayModal.propTypes = {

    modelKey: PropTypes.string.isRequired,
    show: PropTypes.bool.isRequired,
    toggle: PropTypes.func.isRequired
};