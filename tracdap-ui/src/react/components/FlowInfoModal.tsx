/**
 * A component that shows a modal with the full set of information held about a flow.
 *
 * @module FlowInfoModal
 * @category Component
 */

import {Button} from "./Button";
import Col from "react-bootstrap/Col";
import {FlowViewer} from "../scenes/ObjectSummaryScene/components/FlowViewer";
import Modal from "react-bootstrap/Modal";
import type {ObjectSummaryStoreState} from "../scenes/ObjectSummaryScene/store/objectSummaryStore";
import PropTypes from "prop-types";
import Row from "react-bootstrap/Row";
import React from "react";
import {tracdap as trac} from "@finos/tracdap-web-api";

// Some Bootstrap grid layouts
const xlGrid = {span: 10, offset: 1}
const xsGrid = {span: 12, offset: 0}

/**
 * An interface for the props of the FlowInfoModal component.
 */
export interface Props {

    /**
     * Whether to show the modal or not.
     */
    show: boolean
    /**
     * The key in the objectSummaryStore to get the state for the flow visualisation.
     */
    storeKey: keyof ObjectSummaryStoreState["flow"]
    /**
     * The flow object's metadata tag. The ID as well as the tag and object version are needed to
     * be able to get the right version requested by the user.
     */
    tagSelector?: null | trac.metadata.ITagSelector
    /**
     * A function that toggles whether the modal is shown or not.
     */
    toggle: () => void
}

export const FlowInfoModal = (props: Props) => {

    const {show, storeKey, tagSelector, toggle} = props

    return (

        <Modal size={"xl"} show={show} onHide={toggle}>

            <Modal.Header closeButton>
                <Modal.Title>
                    Flow summary
                </Modal.Title>
            </Modal.Header>

            <Modal.Body>

                <Row className={"mb-3"}>
                    <Col xs={xsGrid} xl={xlGrid}>
                        <FlowViewer getTagFromUrl={false} storeKey={storeKey} tagSelector={tagSelector}/>
                    </Col>
                </Row>

            </Modal.Body>
            <Modal.Footer>

                <Button ariaLabel={"Close"}
                        isDispatched={false}
                        onClick={toggle}
                        variant={"secondary"}>
                    Close
                </Button>

            </Modal.Footer>
        </Modal>
    )
};

FlowInfoModal.propTypes = {

    show: PropTypes.bool.isRequired,
    tagSelector: PropTypes.object,
    toggle: PropTypes.func.isRequired
};