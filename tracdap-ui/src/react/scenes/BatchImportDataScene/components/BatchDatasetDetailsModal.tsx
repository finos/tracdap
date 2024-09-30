/**
 * A component that shows a modal with a table with the information about a batch import dataset definition. The user
 * can use this to look at the details about a batch upload before selecting it.
 * @module
 * @category BatchDataImportScene component
 */

import {BatchDatasetDetails} from "./BatchDatasetDetails";
import {Button} from "../../../components/Button";
import Col from "react-bootstrap/Col";
import Modal from "react-bootstrap/Modal"
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row";
import {useAppSelector} from "../../../../types/types_hooks";

// Some Bootstrap grid layouts
const xlGrid = {span: 10, offset: 1}
const xsGrid = {span: 12, offset: 0}

/**
 * An interface for the props of the BatchDatasetDetailsModal component.
 */
interface Props {

    /**
     * The ID for the dataset that related to a specific definition.
     */
    batchDatasetId: string
    /**
     * Whether to show the modal or not.
     */
    show: boolean
    /**
     * A function that toggles whether the modal is shown or not.
     */
    toggle: React.Dispatch<React.SetStateAction<{show: boolean, batchDatasetId: string | null}>>
}

export const BatchDatasetDetailsModal = (props: Props) => {

    const {batchDatasetId, show, toggle} = props

    // Get what we need from te store
    const {data} = useAppSelector(state => state["applicationSetupStore"].tracItems.items.ui_batch_import_data.currentDefinition)

    // Get the definition
    const row = data.find(row => row.DATASET_ID === batchDatasetId)

    return (

        <Modal size={"xl"} show={show} onHide={() => toggle({show: false, batchDatasetId: batchDatasetId})}>

            <Modal.Header closeButton>
                <Modal.Title>
                    Batch import definition for &apos;{row?.DATASET_NAME}&apos;
                </Modal.Title>
            </Modal.Header>

            <Modal.Body>

                <Row className={"mb-3"}>
                    <Col xs={xsGrid} xl={xlGrid}>
                        <BatchDatasetDetails batchImportDefinition={row}/>
                    </Col>
                </Row>

            </Modal.Body>
            <Modal.Footer>

                <Button ariaLabel={"Close"}
                        isDispatched={false}
                        onClick={() => toggle({show: false, batchDatasetId: batchDatasetId})}
                        variant={"secondary"}>
                    Close
                </Button>

            </Modal.Footer>
        </Modal>
    )
};

BatchDatasetDetailsModal.propTypes = {

    batchDatasetId: PropTypes.string,
    show: PropTypes.bool.isRequired,
    toggle: PropTypes.func.isRequired
};