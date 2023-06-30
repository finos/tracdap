/**
 * A component that shows the rules that make up the where clause. This needs to be in a separate
 * component to {@link WhereClauseBuilder} due to the render logic to deal with when the store has not got
 * the object ID already stored for the dataset.
 *
 * @module Overlays
 * @category Component
 */

import Accordion from "react-bootstrap/Accordion";
import {Change} from "./Change";
import Col from "react-bootstrap/Col";
import {ConfirmButton} from "../ConfirmButton";
import {type ConfirmButtonPayload} from "../../../types/types_general";
import {deleteOverlay, type OverlayBuilderStoreState, setDescription} from "./overlayBuilderStore";
import {deleteWhereClause} from "../WhereClauseBuilder/whereClauseBuilderStore";
import ErrorBoundary from "../ErrorBoundary";
import {Icon} from "../Icon";
import {OverlaySql} from "./OverlaySql";
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row";
import {SelectValue} from "../SelectValue";
import Tab from "react-bootstrap/Tab";
import Tabs from "react-bootstrap/Tabs";
import {TextBlock} from "../TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../types/types_hooks";
import {Where} from "../QueryBuilder/Where";
import {WhereClauseBuilder} from "../WhereClauseBuilder/WhereClauseBuilder";

/**
 * An interface for the props of the Overlays component.
 */
export interface Props {

    /**
     * A unique reference to the item having overlays applied to it. In a flow this can be the output node
     * key as these are unique.
     */
    overlayKey: string
    /**
     * The schema of the dataset that the user has selected to query.
     */
    schema: trac.metadata.IFieldSchema[]
    /**
     *  The key in the OverlayBuilderStoreState to use to save the data to / get the data from.
     */
    storeKey: keyof OverlayBuilderStoreState["uses"]
}

export const Overlays = (props: Props) => {

    const {overlayKey, schema, storeKey} = props

    // Get what we need from the store
    const overlay = useAppSelector(state => state["overlayBuilderStore"].uses[storeKey].change[overlayKey])

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    return (

        <ErrorBoundary>
            <Accordion defaultActiveKey="0" className={"mb-4"} alwaysOpen>
                {overlay && overlay.changeTab.map((change, i) =>

                    <Accordion.Item className={`border-end-0 border-start-0 ${change.id}`} eventKey={i.toString()} key={change.id}>
                        <Accordion.Header>
                            Overlay #{i + 1}
                        </Accordion.Header>
                        <Accordion.Body className={"py-4 px-0 mx-0"}>

                            <Row>
                                <Col>
                                    <SelectValue basicType={trac.STRING}
                                                 id={i}
                                                 labelText={"Description of overlay:"}
                                                 minimumValue={10}
                                                 mustValidate={true}
                                                 onChange={setDescription}
                                                 rows={2}
                                                 showValidationMessage={true}
                                                 specialType={"TEXTAREA"}
                                                 validationChecked={change.validationChecked}
                                                 validateOnMount={false}
                                                 value={change.description}
                                    />
                                </Col>
                                <Col xs={"auto"}>
                                    <ConfirmButton ariaLabel={"Delete overlay"}
                                                   cancelText={"No"}
                                                   className={"ms-4 p-0 mt-0 mx-0 mb-1"}
                                                   confirmText={"Yes"}
                                                   description={"Are you sure that you want to delete this overlay?"}
                                                   onClick={(payload: ConfirmButtonPayload) => {
                                                       dispatch(deleteOverlay(payload));
                                                       dispatch(deleteWhereClause(payload))
                                                   }}
                                                   index={i}
                                                   variant={"link"}
                                    >
                                        <Icon ariaLabel={false}
                                              icon={'bi-trash3'}
                                        />
                                    </ConfirmButton>
                                </Col>
                            </Row>

                            <Tabs id={"overlayBuilder"} defaultActiveKey="change">

                                <Tab className={"tab-content-bordered pb-4 min-height-tab-pane "} title={"Change"} eventKey={"change"}>
                                    <Change overlayKey={overlayKey} overlayIndex={i} storeKey={storeKey}/>
                                </Tab>

                                <Tab className={"tab-content-bordered pb-4 min-height-tab-pane "} title={"Where"} eventKey={"where"}>

                                    <TextBlock className={"mt-4 mb-4"}>
                                        Use the button below to add a where clause to your overlay, multiple rules can be combined into a single clause.
                                    </TextBlock>

                                    <WhereClauseBuilder objectKey={overlayKey} schema={schema} storeKey={storeKey} whereIndex={i}/>

                                </Tab>

                                <Tab className={"tab-content-bordered pb-4 min-height-tab-pane "} title={"SQL"} eventKey={"sql"}>
                                    <OverlaySql overlayKey={overlayKey} overlayIndex={i} storeKey={storeKey} whereIndex={i}/>
                                </Tab>
                            </Tabs>

                        </Accordion.Body>
                    </Accordion.Item>
                )}
            </Accordion>
        </ErrorBoundary>
    )
};

Overlays.propTypes = {

    overlayKey: PropTypes.string.isRequired,
    storeKey: PropTypes.string.isRequired
};