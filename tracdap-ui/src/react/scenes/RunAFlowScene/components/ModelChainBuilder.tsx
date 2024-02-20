/**
 * A component that allows the user to view each of the models in a flow and make edits to the models used and
 * apply overlays that are applied at the end of the calculation.
 * @module ModelChainBuilder
 * @category RunAFlowScene component
 */

import {Button} from "../../../components/Button";
import {ButtonPayload} from "../../../../types/types_general";
import Col from "react-bootstrap/Col";
import Collapse from "react-bootstrap/Collapse";
import {getModels, setModelInfoToShow, setShowKeys, setShowModelChain, toggleToShowAllModelInfo, updateOptionLabels} from "../store/runAFlowStore";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {ModelChainCard} from "./ModelChainCard";
import {ModelInfoModal} from "../../../components/ModelInfoModal";
import {ModelOverlayModal} from "./ModelOverlayModal";
import {ModelVersionModal} from "./ModelVersionModal";
import PropTypes from "prop-types";
import React, {memo, useCallback, useReducer} from "react";
import Row from "react-bootstrap/Row";
import {TextBlock} from "../../../components/TextBlock";
import {Toolbar} from "./Toolbar";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";
import {ValidateModels} from "./ValidateModels";

// Some Bootstrap grid layouts
const xlGridModel = {span: 6, offset: 3}
const xlGridSubModel = {span: 5, offset: 4}
const mdGridModel = {span: 10, offset: 1}
const mdGridSubModel = {span: 9, offset: 3}
const xsGridModel = {span: 12, offset: 0}
const xsGridSubModel = {span: 11, offset: 1}

/**
 * The interface for the 'action' argument in the reducer function.
 */
export type Action =
    { type: "showOverlayModal" } |
    { type: "showVersionModal" } |
    { type: "showInfoModal" } |
    { type: "overlayModelKey", key?: string } |
    { type: "versionModelKey", key?: string } |
    { type: "infoModelKey", key?: string }

function reducer(prevState: State, action: Action) {

    switch (action.type) {

        // Using fall through https://stackoverflow.com/questions/6513585/test-for-multiple-cases-in-a-switch-like-an-or
        case "showOverlayModal":
        case "showVersionModal":
        case "showInfoModal":
            return ({
                ...prevState,
                [action.type]: false,
            })

        case "overlayModelKey":
            return ({
                ...prevState,
                [action.type]: action.key,
                showOverlayModal: true
            })

        case "versionModelKey":
            return ({
                ...prevState,
                [action.type]: action.key,
                showVersionModal: true
            })

        case "infoModelKey":
            return ({
                ...prevState,
                [action.type]: action.key,
                showInfoModal: true
            })

        default:
            throw new Error(`Reducer method is not recognised`)
    }
}

/**
 * An interface for the props of the ModelChainBuilder component.
 */
export interface Props {

    /**
     * The css class to apply to the outer div, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * Whether to show the component.
     */
    show: boolean
}

/**
 * An interface for the state of the ModelChainBuilder component.
 */
export interface State {

    /**
     * Which model to load into the overlay modal
     */
    infoModelKey: undefined | string
    /**
     * Which model to load into the overlay modal
     */
    overlayModelKey: undefined | string
    /**
     * Whether to show the info modal
     */
    showInfoModal: boolean
    /**
     * Whether to show the overlay modal
     */
    showOverlayModal: boolean
    /**
     * Whether to show the version modal
     */
    showVersionModal: boolean
    /**
     * Which model to load into the version modal
     */
    versionModelKey: undefined | string,
}

/**
 * The initial state of the component.
 */
const initialState = {
    showOverlayModal: false,
    showVersionModal: false,
    showInfoModal: false,
    overlayModelKey: undefined,
    versionModelKey: undefined,
    infoModelKey: undefined
}

const ModelChainBuilderInner = (props: Props) => {

    console.log("Rendering ModelChainBuilder")

    const {className, show} = props

    // The state initializer and the reducer function to update it
    const [state, updateState] = useReducer(reducer, initialState)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store, note the multiple destructuring is done to prevent
    // re-renders when other parts of the UI update
    const {
        modelOptions,
        modelTypes,
        selectedModelOptions,
        showKeysInsteadOfLabels,
        status
    } = useAppSelector(state => state["runAFlowStore"].models)

    const {modelInfoToShow, showModelChain} = useAppSelector(state => state["runAFlowStore"].modelChain)

    const {
        showCreatedDate,
        showObjectId,
        showUpdatedDate,
        showVersions
    } = useAppSelector(state => state["runAFlowStore"].optionLabels.models)

    /**
     * A function that changes whether the info for a particular model in the model chain should be shown.
     * @param payload - The event that triggered the function.
     */
    const toggleModelInfo = useCallback((payload: ButtonPayload) => {

        if (typeof payload.id === "string") dispatch(setModelInfoToShow(payload.id))

    }, [dispatch])

    /**
     * A function that toggles showing the version selection modal for the user selected model.
     * @param payload - The payload passed by the button clicked.
     */
    const toggleVersionModal = useCallback((payload: ButtonPayload) => {

        if (typeof payload.id === "string") updateState({type: "versionModelKey", key: payload.id})

    }, [])

    /**
     * A function that toggles showing the overlay modal for the user selected model.
     * @param payload - The payload passed by the button clicked.
     */
    const toggleOverlayModal = useCallback((payload: ButtonPayload) => {

        if (typeof payload.id === "string") updateState({type: "overlayModelKey", key: payload.id})

    }, [])

    /**
     * A function that toggles showing the info modal for the user selected model.
     * @param payload - The payload passed by the button clicked.
     */
    const toggleInfoModal = useCallback((payload: ButtonPayload) => {

        if (typeof payload.id === "string") updateState({type: "infoModelKey", key: payload.id})

    }, [])

    return (

        <React.Fragment>
            {/*Has a flow been successfully downloaded*/}
            {show &&

                <React.Fragment>

                    <HeaderTitle type={"h3"} text={"View or edit your model chain (optional)"}/>

                    {Object.keys(modelOptions).length === 0 &&
                        <TextBlock>
                            The selected flow does not contain any models, the model chain can not be edited and will
                            not run.
                        </TextBlock>
                    }

                    {Object.keys(modelOptions).length > 0 &&
                        <React.Fragment>

                            <ValidateModels/>

                            <TextBlock>
                                You can choose to view each of the models selected for use in your flow as well as add
                                overlays to their outputs. By default the models selected will be those compliant with
                                your selected policy, if you change a model then the flow can still be run but the
                                results may be considered out of policy.
                            </TextBlock>

                            <div className={"d-flex justify-content-between"}>
                                <Button ariaLabel={"Show or hide model chain"}
                                        variant={"outline-info"}
                                        className={"mt-2 mb-4 min-width-px-150"}
                                        onClick={setShowModelChain}
                                        isDispatched={true}
                                >
                                    {showModelChain ? "Hide chain" : "Show chain"}
                                </Button>

                                {/*Show the button if the chain is visible*/}
                                {showModelChain &&
                                    <div className={"d-flex"}>
                                        {/*Show the button if there is at least one model with a selected option*/}
                                        {Object.values(selectedModelOptions).some(selectedOption => selectedOption) &&
                                            <Button ariaLabel={"Show or hide all summary info"}
                                                    variant={"outline-info"}
                                                    className={"mt-2 mb-4 min-width-px-150"}
                                                    onClick={toggleToShowAllModelInfo}
                                                    isDispatched={true}
                                            >
                                                {Object.keys(modelInfoToShow).length === Object.keys(modelOptions).length ? "Hide all info" : "Show all info"}
                                            </Button>
                                        }

                                        <div className={"mt-2 ms-2"}>
                                            <Toolbar disabled={Boolean(status === "pending")}
                                                     name={"models"}
                                                     onRefresh={getModels}
                                                     onChangeLabel={updateOptionLabels}
                                                     onShowKeys={setShowKeys}
                                                     showObjectId={showObjectId}
                                                     showVersions={showVersions}
                                                     showCreatedDate={showCreatedDate}
                                                     showUpdatedDate={showUpdatedDate}
                                                     showKeysInsteadOfLabels={showKeysInsteadOfLabels}
                                            />
                                        </div>
                                    </div>
                                }
                            </div>

                            <Collapse in={showModelChain} className={className}>

                                <Row>
                                    {modelTypes.mainModels.map((modelKey) => {

                                        return (
                                            <React.Fragment key={modelKey}>
                                                <Col className={"mb-3"}
                                                     xs={xsGridModel}
                                                     md={mdGridModel}
                                                     xl={xlGridModel}

                                                >
                                                    <ModelChainCard modelKey={modelKey}
                                                                    toggleModelInfo={toggleModelInfo}
                                                                    toggleVersionModal={toggleVersionModal}
                                                                    toggleOverlayModal={toggleOverlayModal}
                                                                    toggleInfoModal={toggleInfoModal}
                                                    />
                                                </Col>

                                                {modelTypes.subModelsByModelKey[modelKey].length > 0 &&
                                                    <React.Fragment>

                                                        <Col className={"my-1"}
                                                             xs={xsGridSubModel}
                                                             md={mdGridSubModel}
                                                             xl={xlGridSubModel}
                                                        >
                                                            <strong>Sub models:</strong>
                                                        </Col>

                                                        {modelTypes.subModelsByModelKey[modelKey].map(subModelKey => {

                                                            return (
                                                                <Col className={"mb-3"}
                                                                     key={subModelKey}
                                                                     xs={xsGridSubModel}
                                                                     md={mdGridSubModel}
                                                                >
                                                                    <ModelChainCard modelKey={subModelKey}
                                                                                    toggleInfoModal={toggleInfoModal}
                                                                                    toggleModelInfo={toggleModelInfo}
                                                                                    toggleOverlayModal={toggleOverlayModal}
                                                                                    toggleVersionModal={toggleVersionModal}
                                                                    />
                                                                </Col>
                                                            )
                                                        })}
                                                    </React.Fragment>
                                                }
                                            </React.Fragment>
                                        )
                                    })}

                                </Row>

                            </Collapse>

                            <ModelInfoModal show={state.showInfoModal}
                                            toggle={updateState}
                                            tagSelector={state.infoModelKey ? selectedModelOptions[state.infoModelKey]?.tag.header : null}
                            />

                            {state.overlayModelKey != undefined &&
                                <ModelOverlayModal modelKey={state.overlayModelKey}
                                                   show={state.showOverlayModal}
                                                   toggle={updateState}
                                />
                            }

                            {state.versionModelKey != undefined &&
                                <ModelVersionModal modelKey={state.versionModelKey}
                                                   show={state.showVersionModal}
                                                   toggle={updateState}
                                />
                            }

                        </React.Fragment>
                    }

                </React.Fragment>}
        </React.Fragment>
    )
};

ModelChainBuilderInner.propTypes = {

    className: PropTypes.string,
    show: PropTypes.bool
};

export const ModelChainBuilder = memo(ModelChainBuilderInner);