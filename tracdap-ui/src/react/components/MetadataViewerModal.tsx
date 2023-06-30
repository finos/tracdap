/**
 * A component that shows a modal with the metadata about an object. This is used when a small
 * subset of metadata is shown to the user in the main page, but you still want the user to be able
 * to access the full metadata should they need to. This is use for example in the JobInfoViewer for
 * the job metadata.
 *
 * @module MetadataViewerModal
 * @category Component
 */

import {Button} from "./Button";
import Col from "react-bootstrap/Col";
import {MetadataViewer} from "./MetadataViewer/MetadataViewer";
import Modal from "react-bootstrap/Modal"
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row";
import {tracdap as trac} from "@finos/tracdap-web-api";

// Some Bootstrap grid layouts
const xlGrid = {span: 10, offset: 1}
const xsGrid = {span: 12, offset: 0}

/**
 * An interface for the props of the MetadataViewerModal component.
 */
export interface Props {

    /**
     * Whether to show the modal or not.
     */
    show: boolean
    /**
     * The object's metadata tag.
     */
    tag: trac.metadata.ITag
    /**
     * The title for the modal.
     */
    title?: string
    /**
     * A function that toggles whether the modal is shown or not.
     */
    toggle: () => void
}

export const MetadataViewerModal = (props: Props) => {

    const {show, tag, title = "Metadata summary", toggle} = props

    return (

        <Modal size={"xl"} show={show} onHide={toggle}>

            <Modal.Header closeButton>
                <Modal.Title>
                    {title}
                </Modal.Title>
            </Modal.Header>

            <Modal.Body>

                <Row className={"mb-3"}>
                    <Col xs={xsGrid} xl={xlGrid}>
                        <MetadataViewer metadata={tag}/>
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

MetadataViewerModal.propTypes = {

    show: PropTypes.bool.isRequired,
    tag: PropTypes.object.isRequired,
    title: PropTypes.string,
    toggle: PropTypes.func.isRequired
};