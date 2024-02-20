/**
 * This component shows a modal with the full set of information held about a model.
 *
 * @module ModelInfoModal
 * @category Component
 */

import {Button} from "./Button";
import type {ButtonPayload} from "../../types/types_general";
import Col from "react-bootstrap/Col";
import Modal from "react-bootstrap/Modal";
import {ModelViewer} from "../scenes/ObjectSummaryScene/components/ModelViewer";
import PropTypes from "prop-types";
import Row from "react-bootstrap/Row";
import React from "react";
import {tracdap as trac} from "@finos/tracdap-web-api";

// Some Bootstrap grid layouts
const xlGrid = {span: 10, offset: 1}
const xsGrid = {span: 12, offset: 0}

/**
 * An interface for the props of the ModelInfoModal component.
 */
export interface Props {

    /**
     * Whether to show the modal or not.
     */
    show: boolean
    /**
     * The model object's metadata tag. The ID as well as the tag and object version are needed to
     * be able to get the right version requested by the user.
     */
    tagSelector?: null | trac.metadata.ITagSelector
    /**
     * A function that toggles whether the modal is shown or not.
     */
    toggle: React.Dispatch<{ type: "showInfoModal" }> | ((payload: ButtonPayload | void) => void)
}

export const ModelInfoModal = (props: Props) => {

    const {show, toggle, tagSelector} = props

    return (

        // TODO can this be done without the binding
        <Modal size={"xl"} show={show} onHide={() => toggle({type: "showInfoModal"})}>

            <Modal.Header closeButton>
                <Modal.Title>
                    Model summary
                </Modal.Title>
            </Modal.Header>

            <Modal.Body>

                <Row className={"mb-3"}>
                    <Col xs={xsGrid} xl={xlGrid}>
                        <ModelViewer getTagFromUrl={false} tagSelector={tagSelector}/>
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

ModelInfoModal.propTypes = {

    show: PropTypes.bool.isRequired,
    toggle: PropTypes.func.isRequired,
    tagHeader: PropTypes.object
};