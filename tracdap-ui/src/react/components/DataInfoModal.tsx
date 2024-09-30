/**
 * A component that shows a modal with the full set of information held about a dataset. This is a wrapper
 * to the {@link DataViewer} component.
 *
 * @module DataInfoModal
 * @category Component
 */

import {Button} from "./Button";
import Col from "react-bootstrap/Col";
import {DataViewer} from "../scenes/ObjectSummaryScene/components/DataViewer";
import Modal from "react-bootstrap/Modal";
import PropTypes from "prop-types";
import Row from "react-bootstrap/Row";
import React from "react";
import {tracdap as trac} from "@finos/tracdap-web-api";

// Some Bootstrap grid layouts
const xlGrid = {span: 10, offset: 1}
const xsGrid = {span: 12, offset: 0}

// TODO Add lineage component at the bottom.
/**
 * An interface for the props of the DataInfoModal component.
 */
export interface Props {

    /**
     * Whether to show the modal or not.
     */
    show: boolean
    /**
     * The dataset object's metadata tag. The ID as well as the tag and object version are needed to
     * be able to get the right version requested by the user.
     */
    tagSelector?: null | trac.metadata.ITagSelector
    /**
     * A function that toggles whether the modal is shown or not.
     */
    toggle: () => void
}

export const DataInfoModal = (props: Props) => {

    const {show, tagSelector, toggle} = props

    return (

        <Modal size={"xl"} show={show} onHide={toggle}>

            <Modal.Header closeButton>
                <Modal.Title>
                    Dataset summary
                </Modal.Title>
            </Modal.Header>

            <Modal.Body>

                <Row className={"mb-3"}>
                    <Col xs={xsGrid} xl={xlGrid}>
                        <DataViewer getTagFromUrl={false} tagSelector={tagSelector}/>
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

DataInfoModal.propTypes = {

    show: PropTypes.bool.isRequired,
    tagHeader: PropTypes.object,
    toggle: PropTypes.func.isRequired
};