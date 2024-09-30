/**
 * A component that shows a card (box) for a model in the model chain builder allowing the user to see the currently
 * selected model and access further information and menus.
 * @module ModelChainCard
 * @category RunAFlowScene component
 */

import Badge from "react-bootstrap/Badge";
import {Button} from "../../../components/Button";
import Card from "react-bootstrap/Card";
import {createUniqueObjectKey, getObjectName} from "../../../utils/utils_trac_metadata";
import {Icon} from "../../../components/Icon";
import {hasOptions} from "../../../utils/utils_general";
import {ObjectDetails} from "../../../components/ObjectDetails";
import React, {memo, useMemo} from "react";
import {useAppSelector} from "../../../../types/types_hooks";

/**
 * An interface for the props of the ModelChainCard component.
 */
export interface Props {

    /**
     * The key of the model node in the flow, this is used as a lookup to get the information
     * to show in the card.
     */
    modelKey: string
    /**
     * The function to run when showing/hiding the modal to allow the user to select the model to use.
     */
    toggleVersionModal: Function
    /**
     * The function to run when showing/hiding the modal to allow the user to add an overlay.
     */
    toggleOverlayModal: Function
    /**
     * The function to run when showing/hiding the modal to show the selected model's full information.
     */
    toggleInfoModal: Function
    /**
     * The function to run when showing/hiding the model's summary information.
     */
    toggleModelInfo: Function
}

const ModelChainCardInner = (props: Props) => {

    const {
        modelKey,
        toggleVersionModal,
        toggleOverlayModal,
        toggleInfoModal,
        toggleModelInfo,
    } = props

    // Get what we need from the store, note the multiple destructuring is done to prevent
    // re-renders when other parts of the UI update
    const {
        modelOptions,
        selectedModelOptions,
        showKeysInsteadOfLabels,
    } = useAppSelector(state => state["runAFlowStore"].models)

    const flowMetadata = useAppSelector(state => state["runAFlowStore"].flow.flowMetadata)
    const isValid = useAppSelector(state => state["runAFlowStore"].validation.isValid.models[modelKey])
    const validationChecked = useAppSelector(state => state["runAFlowStore"].validation.validationChecked)
    const showModelInfo = useAppSelector(state => state["runAFlowStore"].modelChain.modelInfoToShow[modelKey])
    const {job} = useAppSelector(state => state["runAFlowStore"].rerun)
    const {change: overlaysByOutputKey} = useAppSelector(state => state["overlayBuilderStore"].uses.runAFlow)

    // The node in the flow that the card represents
    const flowNode = flowMetadata?.definition?.flow?.nodes?.[modelKey]
    // What model options are available
    const options = modelOptions[modelKey]
    // The model selected.
    const selectedModelOption = selectedModelOptions[modelKey]
    // The policy selected.
    const selectedPolicyOption = job?.definition?.job?.runFlow?.models?.[modelKey] ?? null

    /**
     * A memoized value for the number of overlays applied.
     */
    const numberOfOverlays = useMemo(() => {

        const modelOutputKeys = selectedModelOption?.tag?.definition?.model?.outputs ? Object.keys(selectedModelOption.tag.definition.model.outputs) : []

        let overlays = 0
        modelOutputKeys.forEach(outputKey => {
            if (Object.keys(overlaysByOutputKey).includes(outputKey)) {
                overlays = overlays + overlaysByOutputKey[outputKey].changeTab.length
            }
        })
        return overlays
        
    }, [overlaysByOutputKey, selectedModelOption])

    /**
     * A function to set the header title for the card.
     */
    const getHeader = (): string => {

        if (showKeysInsteadOfLabels) {
            return modelKey
        } else if (flowNode?.label != null) {
            return flowNode?.label
        } else if (!selectedModelOption) {
            return modelKey
        } else {
            return getObjectName(selectedModelOption.tag, false, false, false, false)
        }
    }

    return (

        // If the selection is invalid (e.g. one is required but not set) then border to card in red to show the user that there is a problem
        // This only shows when validation checked is true.
        <Card className={`model-card ${!isValid && validationChecked ? "is-invalid" : "border-1"}`}>

            <Card.Header className={"model-card-header px-3 py-2 border-bottom"}>
                {getHeader()}
            </Card.Header>

            <Card.Body className={"model-card-body px-3 py-2"}>
                <div className={"w-100 d-flex justify-content-between align-items-center"}>

                    {!selectedModelOption &&
                        <div>
                            {hasOptions(options) ? "No model has been selected" : "No models for this flow are available"}
                        </div>
                    }

                    {selectedModelOption &&
                        <Button ariaLabel={"Info"}
                                className={"ms-0 ps-0"}
                                id={modelKey}
                                isDispatched={false}
                                onClick={toggleModelInfo}
                                variant={"dark"}
                        >
                            <Icon ariaLabel={false}
                                  className={"me-2"}
                                  icon={showModelInfo ? "bi-caret-down-fill" : "bi-caret-right-fill"}
                            />
                            Summary Info
                        </Button>
                    }
                    <div>
                        {selectedModelOption &&
                            <Button className={"me-2"}
                                    ariaLabel={"Overlay model"}
                                    id={modelKey}
                                    isDispatched={false}
                                    onClick={toggleOverlayModal}

                                    variant={"dark"}
                            >
                                <Icon ariaLabel={false}
                                      className={"me-2"}
                                      icon={"bi-pencil-square"}
                                />
                                Overlay
                                {numberOfOverlays > 0 ?
                                    <React.Fragment>
                                        <Badge bg="danger" className={"ms-2"}>{numberOfOverlays}</Badge>
                                        <span className="visually-hidden">overlays applied</span>
                                    </React.Fragment>
                                : null}

                            </Button>
                        }

                        {selectedModelOption &&
                            <Button ariaLabel={"Full details"}
                                    className={"me-2"}
                                    id={modelKey}
                                    isDispatched={false}
                                    onClick={toggleInfoModal}
                                    variant={"dark"}
                            >
                                <Icon ariaLabel={false}
                                      className={"me-2"}
                                      icon={"bi-info-circle"}
                                />
                                Full details
                            </Button>
                        }

                        <Button className={"me-0 pe-0"}
                                ariaLabel={"Version model"}
                                id={modelKey}
                                isDispatched={false}
                                onClick={toggleVersionModal}
                                variant={"dark"}
                        >
                            <Icon ariaLabel={false}
                                  className={"me-2"}
                                  icon={"bi-git"}
                            />
                            Change version
                        </Button>

                    </div>

                </div>

                {showModelInfo && selectedModelOption &&
                    <ObjectDetails metadata={selectedModelOption.tag} bordered={false} striped={false} variant={"dark"}/>
                }

            </Card.Body>

            <Card.Footer className={`px-3 py-2 model-card-footer border-top`}>
                {!selectedPolicyOption &&
                    <div>
                        <Icon ariaLabel={false}
                              className={"me-3 "}
                              icon={"bi-info-circle"}
                        />
                        <span>No policy selected</span>
                    </div>
                }

                {selectedPolicyOption &&
                    <React.Fragment>
                        {selectedModelOption?.tag?.header && createUniqueObjectKey(selectedPolicyOption, false) === createUniqueObjectKey(selectedModelOption.tag.header, false) ?
                            <div className={"text-success-inverted-background"}>
                                <Icon ariaLabel={false}
                                      className={"me-3"}
                                      icon={"bi-check-circle"}
                                />
                                <span>In policy</span>
                            </div>
                            :
                            <div className={"text-danger-inverted-background"}>
                                <Icon ariaLabel={false}
                                      className={"me-3"}
                                      icon={"bi-x-circle"}
                                />
                                <span>Not in policy</span>
                            </div>
                        }
                    </React.Fragment>
                }
            </Card.Footer>
        </Card>
    )
};

export const ModelChainCard = memo(ModelChainCardInner);